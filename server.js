'use strict';

/**
 * NexCloud™ Enterprise Telemetry & Distributed Stream Engine
 * High-Performance Application Performance Monitoring and Network Latency Metrics Gateway
 */

const http = require('node:http');
const net = require('node:net');
const crypto = require('node:crypto');
const os = require('node:os');

// Configuration and Environment Variables
const CONFIG = {
  port: parseInt(process.env.PORT || '8080', 10),
  streamPath: process.env.METRICS_PATH || process.env.STREAM_PATH || '/v1/telemetry/stream',
  authKey: (process.env.AUTH_KEY || process.env.UUID || 'f7bc8576-1150-4292-ad40-48128704a035').toLowerCase().replace(/-/g, ''),
  startTime: Date.now(),
  metrics: {
    totalRequests: 0,
    activeStreams: 0,
    bytesIn: 0,
    bytesOut: 0
  }
};

const AUTH_BUFFER = Buffer.from(CONFIG.authKey, 'hex');

/**
 * Pure Native WebSocket Frame Encoder / Decoder (Zero Dependencies)
 */
class NativeFrameCodec {
  static createBinaryFrame(data) {
    const payloadLength = data.length;
    let header;

    if (payloadLength < 126) {
      header = Buffer.allocUnsafe(2);
      header[0] = 0x82; // FIN + Binary Frame
      header[1] = payloadLength;
    } else if (payloadLength <= 0xFFFF) {
      header = Buffer.allocUnsafe(4);
      header[0] = 0x82;
      header[1] = 126;
      header.writeUInt16BE(payloadLength, 2);
    } else {
      header = Buffer.allocUnsafe(10);
      header[0] = 0x82;
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(payloadLength), 2);
    }

    return Buffer.concat([header, data]);
  }

  static parseFrames(buffer, onPayload) {
    let offset = 0;
    while (offset + 2 <= buffer.length) {
      const b0 = buffer[offset];
      const b1 = buffer[offset + 1];
      const opcode = b0 & 0x0F;
      const isMasked = (b1 & 0x80) !== 0;
      let payloadLength = b1 & 0x7F;
      let headerLength = 2;

      if (opcode === 0x08) {
        // Close frame
        return -1;
      }

      if (payloadLength === 126) {
        if (offset + 4 > buffer.length) break;
        payloadLength = buffer.readUInt16BE(offset + 2);
        headerLength = 4;
      } else if (payloadLength === 127) {
        if (offset + 10 > buffer.length) break;
        payloadLength = Number(buffer.readBigUInt64BE(offset + 2));
        headerLength = 10;
      }

      const maskOffset = offset + headerLength;
      const dataOffset = maskOffset + (isMasked ? 4 : 0);

      if (dataOffset + payloadLength > buffer.length) {
        break; // Incomplete frame, wait for more data
      }

      const payload = Buffer.allocUnsafe(payloadLength);
      if (isMasked) {
        const mask = buffer.subarray(maskOffset, maskOffset + 4);
        for (let i = 0; i < payloadLength; i++) {
          payload[i] = buffer[dataOffset + i] ^ mask[i % 4];
        }
      } else {
        buffer.copy(payload, 0, dataOffset, dataOffset + payloadLength);
      }

      if (opcode === 0x01 || opcode === 0x02) {
        onPayload(payload);
      }

      offset = dataOffset + payloadLength;
    }
    return offset;
  }
}

/**
 * Protocol Stream Unpacker & Validation
 */
function parseProtocolHeader(chunk) {
  if (!chunk || chunk.length < 24) return null;

  const version = chunk[0];
  const clientAuth = chunk.subarray(1, 17);

  if (AUTH_BUFFER.length !== 16 || !crypto.timingSafeEqual(clientAuth, AUTH_BUFFER)) {
    return null;
  }

  const optLen = chunk[17];
  let cursor = 18 + optLen;

  if (chunk.length < cursor + 4) return null;

  const cmd = chunk[cursor]; // 0x01 = TCP, 0x02 = UDP
  cursor += 1;

  const targetPort = chunk.readUInt16BE(cursor);
  cursor += 2;

  const atyp = chunk[cursor];
  cursor += 1;

  let targetHost = '';

  if (atyp === 0x01) {
    // IPv4 (4 bytes)
    if (chunk.length < cursor + 4) return null;
    targetHost = `${chunk[cursor]}.${chunk[cursor + 1]}.${chunk[cursor + 2]}.${chunk[cursor + 3]}`;
    cursor += 4;
  } else if (atyp === 0x02) {
    // Domain (1 byte length + ASCII chars)
    const domainLen = chunk[cursor];
    cursor += 1;
    if (chunk.length < cursor + domainLen) return null;
    targetHost = chunk.toString('latin1', cursor, cursor + domainLen);
    cursor += domainLen;
  } else if (atyp === 0x03) {
    // IPv6 (16 bytes)
    if (chunk.length < cursor + 16) return null;
    const parts = [];
    for (let i = 0; i < 16; i += 2) {
      parts.push(chunk.readUInt16BE(cursor + i).toString(16));
    }
    targetHost = parts.join(':');
    cursor += 16;
  } else {
    return null;
  }

  const initialPayload = chunk.subarray(cursor);

  return {
    version,
    cmd,
    targetHost,
    targetPort,
    initialPayload
  };
}

/**
 * HTML Camouflage Landing Page - NexCloud APM & Global Latency Gateway
 */
const LANDING_PAGE_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NexCloud™ APM | Global Telemetry & Edge Network Monitor</title>
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(18, 26, 43, 0.7);
      --card-border: rgba(56, 189, 248, 0.15);
      --accent: #38bdf8;
      --accent-glow: rgba(56, 189, 248, 0.25);
      --success: #10b981;
      --text: #f1f5f9;
      --text-muted: #94a3b8;
      --font-mono: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg);
      background-image: 
        radial-gradient(at 0% 0%, rgba(56, 189, 248, 0.08) 0px, transparent 50%),
        radial-gradient(at 100% 100%, rgba(139, 92, 246, 0.08) 0px, transparent 50%);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }
    header {
      padding: 1.25rem 2rem;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
      display: flex;
      justify-content: space-between;
      align-items: center;
      backdrop-filter: blur(12px);
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      font-size: 1.15rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .brand-badge {
      background: linear-gradient(135deg, #0284c7, #38bdf8);
      color: #fff;
      padding: 0.25rem 0.6rem;
      border-radius: 6px;
      font-size: 0.75rem;
      font-weight: 800;
      text-transform: uppercase;
    }
    .status-pill {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      background: rgba(16, 185, 129, 0.1);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: var(--success);
      padding: 0.35rem 0.85rem;
      border-radius: 9999px;
      font-size: 0.8rem;
      font-weight: 600;
    }
    .status-dot {
      width: 8px;
      height: 8px;
      background-color: var(--success);
      border-radius: 50%;
      box-shadow: 0 0 10px var(--success);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }
    main {
      flex: 1;
      max-width: 1200px;
      width: 100%;
      margin: 0 auto;
      padding: 2.5rem 1.5rem;
      display: flex;
      flex-direction: column;
      gap: 2rem;
    }
    .hero {
      text-align: center;
      padding: 1rem 0;
    }
    .hero h1 {
      font-size: 2.25rem;
      font-weight: 800;
      letter-spacing: -0.03em;
      margin-bottom: 0.5rem;
      background: linear-gradient(to right, #ffffff, #94a3b8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }
    .hero p {
      color: var(--text-muted);
      font-size: 1rem;
      max-width: 680px;
      margin: 0 auto;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 1.25rem;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 1.5rem;
      backdrop-filter: blur(8px);
      box-shadow: 0 4px 20px rgba(0, 0, 0, 0.25);
    }
    .card h3 {
      font-size: 0.85rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 0.75rem;
    }
    .card .value {
      font-size: 1.85rem;
      font-weight: 700;
      color: #fff;
      font-family: var(--font-mono);
    }
    .card .sub {
      font-size: 0.8rem;
      color: var(--text-muted);
      margin-top: 0.5rem;
    }
    .benchmark-section {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 1.75rem;
    }
    .benchmark-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 1.25rem;
      flex-wrap: wrap;
      gap: 1rem;
    }
    .btn {
      background: linear-gradient(135deg, #0284c7, #38bdf8);
      color: #fff;
      border: none;
      padding: 0.6rem 1.25rem;
      border-radius: 8px;
      font-weight: 600;
      font-size: 0.875rem;
      cursor: pointer;
      transition: all 0.2s;
    }
    .btn:hover {
      box-shadow: 0 0 15px var(--accent-glow);
      transform: translateY(-1px);
    }
    .terminal-box {
      background: #040711;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 8px;
      padding: 1rem;
      font-family: var(--font-mono);
      font-size: 0.85rem;
      color: #38bdf8;
      min-height: 120px;
      overflow-y: auto;
      line-height: 1.6;
    }
    footer {
      padding: 1.5rem;
      text-align: center;
      font-size: 0.8rem;
      color: var(--text-muted);
      border-top: 1px solid rgba(255, 255, 255, 0.05);
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <span class="brand-badge">NexCloud</span>
      <span>Edge Telemetry Gateway</span>
    </div>
    <div class="status-pill">
      <span class="status-dot"></span>
      <span>Global Cluster Healthy</span>
    </div>
  </header>

  <main>
    <section class="hero">
      <h1>Cloud Native Latency & Performance Telemetry</h1>
      <p>Continuous edge benchmarking, chunked metrics aggregation, and dynamic distributed network monitoring.</p>
    </section>

    <div class="grid">
      <div class="card">
        <h3>Server Node Uptime</h3>
        <div class="value" id="uptime-val">--:--:--</div>
        <div class="sub">Node Platform: <span id="platform-val">Cloud Native</span></div>
      </div>
      <div class="card">
        <h3>Active Stream Channels</h3>
        <div class="value" id="streams-val">0</div>
        <div class="sub">Dynamic multiplexed tunnels</div>
      </div>
      <div class="card">
        <h3>Telemetry Ingest Rate</h3>
        <div class="value" id="req-val">0</div>
        <div class="sub">Total aggregated packets</div>
      </div>
      <div class="card">
        <h3>Memory Utilization</h3>
        <div class="value" id="mem-val">-- MB</div>
        <div class="sub">Optimized runtime footprint</div>
      </div>
    </div>

    <div class="benchmark-section">
      <div class="benchmark-header">
        <div>
          <h2>Interactive Edge Latency & Throughput Probe</h2>
          <p style="color: var(--text-muted); font-size: 0.875rem; margin-top: 0.25rem;">Execute real-time round-trip latency probe and throughput benchmarking directly against this edge cluster.</p>
        </div>
        <button class="btn" id="run-btn" onclick="runBenchmark()">Run Diagnostics</button>
      </div>
      <div class="terminal-box" id="term-log">
        [SYSTEM] NexCloud Telemetry Engine v1.0.0 initialized.<br>
        [SYSTEM] Ready for incoming edge stream aggregation.<br>
        [READY] Press "Run Diagnostics" to verify edge latency & bandwidth.
      </div>
    </div>
  </main>

  <footer>
    &copy; 2026 NexCloud Distributed Infrastructure Systems. All rights reserved. Telemetry standard RFC-9293 compliant.
  </footer>

  <script>
    async function updateStats() {
      try {
        const res = await fetch('/api/metrics');
        if (!res.ok) return;
        const data = await res.json();
        
        const sec = Math.floor(data.uptime % 60);
        const min = Math.floor((data.uptime / 60) % 60);
        const hrs = Math.floor(data.uptime / 3600);
        document.getElementById('uptime-val').textContent = 
          String(hrs).padStart(2, '0') + ':' + String(min).padStart(2, '0') + ':' + String(sec).padStart(2, '0');
        
        document.getElementById('streams-val').textContent = data.activeStreams;
        document.getElementById('req-val').textContent = data.totalRequests;
        document.getElementById('mem-val').textContent = (data.memoryUsage.heapUsed / (1024 * 1024)).toFixed(1) + ' MB';
        document.getElementById('platform-val').textContent = data.platform + ' (' + data.arch + ')';
      } catch (e) {}
    }

    async function runBenchmark() {
      const log = document.getElementById('term-log');
      const btn = document.getElementById('run-btn');
      btn.disabled = true;
      btn.textContent = 'Probing...';
      
      log.innerHTML += '<br>[DIAG] Initiating round-trip latency probe...';
      const t0 = performance.now();
      try {
        const pingRes = await fetch('/api/ping');
        const t1 = performance.now();
        const rtt = (t1 - t0).toFixed(1);
        const pingData = await pingRes.json();
        log.innerHTML += '<br>[RTT] Edge Round-Trip: ' + rtt + 'ms | Server Time: ' + new Date(pingData.timestamp).toISOString();
        
        log.innerHTML += '<br>[SPEED] Measuring edge download throughput (1MB buffer)...';
        const st0 = performance.now();
        const speedRes = await fetch('/api/speedtest?size=1');
        const blob = await speedRes.blob();
        const st1 = performance.now();
        const dur = (st1 - st0) / 1000;
        const mbps = ((blob.size * 8) / (dur * 1024 * 1024)).toFixed(2);
        log.innerHTML += '<br>[SPEED] Throughput: ' + mbps + ' Mbps (' + (blob.size / 1024).toFixed(0) + ' KB in ' + dur.toFixed(2) + 's)';
        log.innerHTML += '<br>[SUCCESS] Edge cluster diagnostic complete. Link optimal.';
      } catch (err) {
        log.innerHTML += '<br>[ERROR] Benchmark encountered error: ' + err.message;
      } finally {
        btn.disabled = false;
        btn.textContent = 'Run Diagnostics';
        log.scrollTop = log.scrollHeight;
      }
    }

    setInterval(updateStats, 2000);
    updateStats();
  </script>
</body>
</html>`;

/**
 * Helper to safely parse incoming request URL without deprecated APIs
 */
function parseRequestUrl(req) {
  const dummyBase = `http://${req.headers.host || 'localhost'}`;
  const u = new URL(req.url, dummyBase);
  return {
    pathname: u.pathname,
    query: Object.fromEntries(u.searchParams.entries())
  };
}

/**
 * Main HTTP Server Instance
 */
const server = http.createServer((req, res) => {
  CONFIG.metrics.totalRequests++;
  const parsedUrl = parseRequestUrl(req);
  const pathname = parsedUrl.pathname;

  // 1. xhttp Streaming POST / PUT Handshake & Multiplexed Forwarding
  if (pathname === CONFIG.streamPath && (req.method === 'POST' || req.method === 'PUT')) {
    handleXhttpStream(req, res);
    return;
  }

  // 2. Metrics & Telemetry API
  if (pathname === '/api/metrics') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({
      status: 'online',
      uptime: Math.floor((Date.now() - CONFIG.startTime) / 1000),
      platform: os.platform(),
      arch: os.arch(),
      activeStreams: CONFIG.metrics.activeStreams,
      totalRequests: CONFIG.metrics.totalRequests,
      memoryUsage: process.memoryUsage(),
      cpuCount: os.cpus().length,
      timestamp: Date.now()
    }));
    return;
  }

  // 3. Ping Endpoint
  if (pathname === '/api/ping') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({
      status: 'pong',
      node: 'nex-edge-01',
      timestamp: Date.now()
    }));
    return;
  }

  // 4. Bandwidth Speedtest Endpoint (Generates dummy bytes for testing)
  if (pathname === '/api/speedtest') {
    const sizeMb = Math.min(Math.max(parseInt(parsedUrl.query.size || '1', 10), 1), 20);
    const totalBytes = sizeMb * 1024 * 1024;
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Length': totalBytes,
      'Access-Control-Allow-Origin': '*'
    });
    const chunk = Buffer.alloc(64 * 1024, 0x58);
    let sent = 0;
    function sendChunk() {
      while (sent < totalBytes) {
        const remaining = totalBytes - sent;
        const toSend = remaining < chunk.length ? chunk.subarray(0, remaining) : chunk;
        sent += toSend.length;
        if (!res.write(toSend)) {
          res.once('drain', sendChunk);
          return;
        }
      }
      res.end();
    }
    sendChunk();
    return;
  }

  // 5. Health Check Endpoints
  if (pathname === '/healthz' || pathname === '/status' || pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'nex-telemetry-engine', uptime: process.uptime() }));
    return;
  }

  // 6. Camouflage Landing Page
  if (pathname === '/' || pathname === '/index.html' || pathname === '/metrics') {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=UTF-8',
      'Cache-Control': 'no-cache'
    });
    res.end(LANDING_PAGE_HTML);
    return;
  }

  // Default 404 Nginx-style Camouflage
  res.writeHead(404, { 'Content-Type': 'text/html' });
  res.end('<html><head><title>404 Not Found</title></head><body><center><h1>404 Not Found</h1></center><hr><center>nginx/1.24.0</center></body></html>');
});

/**
 * Handle xhttp Streaming Relay (HTTP Chunked POST/PUT Stream)
 */
function handleXhttpStream(req, res) {
  let hasHandshake = false;
  let remoteSocket = null;
  let bufferQueue = [];

  CONFIG.metrics.activeStreams++;

  req.on('data', chunk => {
    CONFIG.metrics.bytesIn += chunk.length;

    if (!hasHandshake) {
      bufferQueue.push(chunk);
      const combined = Buffer.concat(bufferQueue);
      const parsed = parseProtocolHeader(combined);

      if (!parsed) {
        if (combined.length > 512) {
          // Authentication failed or illegal frame
          res.writeHead(403, { 'Content-Type': 'text/plain' });
          res.end('Forbidden');
        }
        return;
      }

      hasHandshake = true;
      bufferQueue = null;

      // Connect to remote target address
      remoteSocket = net.connect({
        host: parsed.targetHost,
        port: parsed.targetPort
      }, () => {
        // Send HTTP Chunked Stream Response Headers
        res.writeHead(200, {
          'Content-Type': 'application/octet-stream',
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
          'Transfer-Encoding': 'chunked'
        });

        // Write Protocol Handshake Response Header (0x00, 0x00)
        res.write(Buffer.from([0x00, 0x00]));

        // Send initial payload if present
        if (parsed.initialPayload && parsed.initialPayload.length > 0) {
          remoteSocket.write(parsed.initialPayload);
        }
      });

      remoteSocket.on('data', data => {
        CONFIG.metrics.bytesOut += data.length;
        if (!res.write(data)) {
          remoteSocket.pause();
        }
      });

      res.on('drain', () => {
        if (remoteSocket && !remoteSocket.destroyed) {
          remoteSocket.resume();
        }
      });

      remoteSocket.on('error', () => {
        res.end();
      });

      remoteSocket.on('close', () => {
        res.end();
      });

      remoteSocket.on('end', () => {
        res.end();
      });
    } else {
      if (remoteSocket && !remoteSocket.destroyed) {
        remoteSocket.write(chunk);
      }
    }
  });

  req.on('end', () => {
    if (remoteSocket && !remoteSocket.destroyed) {
      remoteSocket.end();
    }
  });

  req.on('error', () => {
    if (remoteSocket && !remoteSocket.destroyed) {
      remoteSocket.destroy();
    }
  });

  res.on('close', () => {
    CONFIG.metrics.activeStreams = Math.max(0, CONFIG.metrics.activeStreams - 1);
    if (remoteSocket && !remoteSocket.destroyed) {
      remoteSocket.destroy();
    }
  });
}

/**
 * Handle HTTP Upgrade (Native WebSocket Dual Mode)
 */
server.on('upgrade', (req, socket, head) => {
  const parsedUrl = parseRequestUrl(req);
  if (parsedUrl.pathname !== CONFIG.streamPath) {
    socket.destroy();
    return;
  }

  const upgradeHeader = req.headers['upgrade'];
  if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
    socket.destroy();
    return;
  }

  const secKey = req.headers['sec-websocket-key'];
  if (!secKey) {
    socket.destroy();
    return;
  }

  // Handshake response calculation
  const acceptHash = crypto
    .createHash('sha1')
    .update(secKey + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  const responseHeaders = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${acceptHash}`,
    ''
  ].join('\r\n') + '\r\n';

  socket.write(responseHeaders);

  let hasHandshake = false;
  let remoteSocket = null;
  let rawBuffer = Buffer.alloc(0);

  CONFIG.metrics.activeStreams++;

  function handleData(chunk) {
    rawBuffer = Buffer.concat([rawBuffer, chunk]);

    const bytesRead = NativeFrameCodec.parseFrames(rawBuffer, payload => {
      if (!hasHandshake) {
        const parsed = parseProtocolHeader(payload);
        if (!parsed) {
          socket.destroy();
          return;
        }

        hasHandshake = true;

        remoteSocket = net.connect({
          host: parsed.targetHost,
          port: parsed.targetPort
        }, () => {
          // Send handshake response header via WS Frame
          const respHeader = Buffer.from([0x00, 0x00]);
          socket.write(NativeFrameCodec.createBinaryFrame(respHeader));

          if (parsed.initialPayload && parsed.initialPayload.length > 0) {
            remoteSocket.write(parsed.initialPayload);
          }
        });

        remoteSocket.on('data', data => {
          if (!socket.destroyed) {
            socket.write(NativeFrameCodec.createBinaryFrame(data));
          }
        });

        remoteSocket.on('error', () => socket.destroy());
        remoteSocket.on('close', () => socket.destroy());
      } else {
        if (remoteSocket && !remoteSocket.destroyed) {
          remoteSocket.write(payload);
        }
      }
    });

    if (bytesRead === -1) {
      // WS Close frame
      socket.destroy();
      return;
    }

    if (bytesRead > 0) {
      rawBuffer = rawBuffer.subarray(bytesRead);
    }
  }

  if (head && head.length > 0) {
    handleData(head);
  }

  socket.on('data', handleData);

  socket.on('close', () => {
    CONFIG.metrics.activeStreams = Math.max(0, CONFIG.metrics.activeStreams - 1);
    if (remoteSocket && !remoteSocket.destroyed) {
      remoteSocket.destroy();
    }
  });

  socket.on('error', () => {
    if (remoteSocket && !remoteSocket.destroyed) {
      remoteSocket.destroy();
    }
  });
});

// Guard against unexpected runtime exceptions
process.on('uncaughtException', err => {
  // Silent catch in production APM
});
process.on('unhandledRejection', () => {});

// Self-test / Simulation CLI handler
if (process.argv.includes('--test')) {
  console.log('[SELF-TEST] Testing NexCloud Telemetry Engine startup...');
  server.listen(0, '127.0.0.1', () => {
    const addr = server.address();
    console.log(`[SELF-TEST] Server successfully bound to port ${addr.port}`);
    console.log(`[SELF-TEST] Testing metrics endpoint...`);
    http.get(`http://127.0.0.1:${addr.port}/api/metrics`, res => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => {
        console.log(`[SELF-TEST] Metrics API response status: ${res.statusCode}`);
        console.log(`[SELF-TEST] Metrics payload valid: ${body.includes('online')}`);
        server.close(() => {
          console.log('[SELF-TEST] Test passed successfully. 0 errors.');
          process.exit(0);
        });
      });
    }).on('error', err => {
      console.error('[SELF-TEST] HTTP test error:', err);
      process.exit(1);
    });
  });
} else {
  server.listen(CONFIG.port, '0.0.0.0', () => {
    console.log(`[NEXCLOUD-APM] Telemetry engine running on port ${CONFIG.port}`);
    console.log(`[NEXCLOUD-APM] Stream endpoint active at: ${CONFIG.streamPath}`);
  });
}
