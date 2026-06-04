const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const url = require('url');
const LOG_FILE = path.join(__dirname, 'server.log');

// ============================================================
// CLIENT REGISTRY - tracks all connected C# WebSocket clients
// ============================================================
const clients = new Map();

function toIsoLikeLogDate() {
  const now = new Date();
  const yy = String(now.getFullYear() % 100).padStart(2, '0');
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  const hh = String(now.getHours()).padStart(2, '0');
  const min = String(now.getMinutes()).padStart(2, '0');
  return `${yy}:${mm}:${dd} ${hh}:${min}`;
}

function toIpv4(ip) {
  if (!ip) return 'Unknown';
  if (ip === '::1') return '127.0.0.1';
  if (ip.startsWith('::ffff:')) return ip.substring(7);
  if (/^\d+\.\d+\.\d+\.\d+$/.test(ip)) return ip;
  if (ip.includes(':') && ip.includes('.')) {
    const last = ip.split(':').pop();
    return /^\d+\.\d+\.\d+\.\d+$/.test(last) ? last : 'Unknown';
  }
  return 'Unknown';
}

function writeAuditLog(event, data) {
  const user = (data && data.user) || 'Unknown';
  const ip = toIpv4((data && data.ip) || 'Unknown');
  const action = event === 'USER_CONNECTED' ? 'connect' : 'disconnect';
  const line = `${toIsoLikeLogDate()}, ${action}, ${user}, ${ip}`;
  console.log(line);
  fs.appendFile(LOG_FILE, line + '\n', () => {});
}

// ============================================================
// HTTP SERVER
// ============================================================
const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url, true);

  if (parsedUrl.pathname === '/' || parsedUrl.pathname === '/dashboard') {
    const html = fs.readFileSync(path.join(__dirname, 'dashboard.html'), 'utf-8');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
  } else if (parsedUrl.pathname === '/api/clients') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const list = [];
    for (const [user, c] of clients) {
      list.push({ username: user, ip: c.ip, connectedAt: c.connectedAt, state: c.state });
    }
    res.end(JSON.stringify(list));
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

// ============================================================
// WEBSOCKET SERVER - C# clients + browser dashboard
// ============================================================
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  const clientIP = req.socket.remoteAddress;
  const clientIPv4 = toIpv4(clientIP);
  let clientUsername = null;
  let isDashboard = false;

  ws.on('message', (data, isBinary) => {
    const isDataBinary = isBinary === true;
    if (isDataBinary) {
      if (ws._dashboard && ws._dashboard.readyState === WebSocket.OPEN) {
        ws._dashboard.send(data);
      } else if (ws._client && ws._client.readyState === WebSocket.OPEN) {
        ws._client.send(data);
      }
      return;
    }

    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) {
      if (isBinary === undefined) {
        if (ws._dashboard && ws._dashboard.readyState === WebSocket.OPEN) {
          ws._dashboard.send(data);
        } else if (ws._client && ws._client.readyState === WebSocket.OPEN) {
          ws._client.send(data);
        }
      }
      return;
    }

    if (msg.type === 'DASHBOARD') {
      isDashboard = true;
      ws._isDashboard = true;
      broadcastClientList();
      return;
    }

    if (msg.type === 'DASH_CMD') {
      const target = clients.get(msg.target);
      if (target && target.ws.readyState === WebSocket.OPEN) {
        target.ws._dashboard = ws;
        ws._client = target.ws;
        ws._targetUser = msg.target;
        target.ws.send(JSON.stringify(msg.command));
      } else {
        ws.send(JSON.stringify({ type: 'ERROR', payload: `User "${msg.target}" not connected`, timestamp: new Date().toISOString() }));
      }
      return;
    }

    if (msg.type === 'AUTH') {
      clientUsername = msg.payload;
      ws._username = clientUsername;
      const old = clients.get(clientUsername);
      if (old) { try { old.ws.close(); } catch (e) {} }
      clients.set(clientUsername, { ws, ip: clientIPv4, username: clientUsername, connectedAt: new Date().toISOString(), state: 'connected' });
      writeAuditLog('USER_CONNECTED', { user: clientUsername, ip: clientIPv4 });
      broadcastClientList();
      return;
    }

    if (clientUsername && ws._dashboard && ws._dashboard.readyState === WebSocket.OPEN) {
      msg._from = clientUsername;
      ws._dashboard.send(JSON.stringify(msg));
    }
  });

  ws.on('close', () => {
    if (isDashboard) {
      if (ws._client && ws._client._dashboard === ws) {
        ws._client._dashboard = null;
      }
    } else if (clientUsername) {
      const entry = clients.get(clientUsername);
      if (entry) {
        entry.ws._dashboard = null;
        entry.state = 'disconnected';
        broadcastClientList();
        setTimeout(() => {
          if (clients.get(clientUsername)?.state === 'disconnected') {
            clients.delete(clientUsername);
            broadcastClientList();
          }
        }, 5000);
      }
      writeAuditLog('USER_DISCONNECTED', { user: clientUsername, ip: clientIPv4 });
    }
  });

  ws.on('error', (_err) => { /* no-op */ });
});

function broadcastClientList() {
  const list = [];
  for (const [user, c] of clients) {
    list.push({ username: user, ip: c.ip, connectedAt: c.connectedAt, state: c.state });
  }
  const payload = JSON.stringify({ type: 'CLIENT_LIST', clients: list });
  wss.clients.forEach((c) => {
    if (c._isDashboard && c.readyState === WebSocket.OPEN) c.send(payload);
  });
}

// ============================================================
// START
// ============================================================
const PORT = process.env.PORT || 2222;
server.listen(PORT, () => {
  // Intentionally no general startup logs.
});

process.on('SIGINT', () => {
  wss.clients.forEach(c => c.close());
  server.close(() => process.exit(0));
});
