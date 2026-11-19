const WebSocket = require('ws');
const express = require('express');
const path = require('path');
const http = require('http');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

const NODE_TIMEOUT = 30000;
const HEARTBEAT_INTERVAL = 5000;

let nodes = new Map();
let taskQueue = [];
let currentTask = null;
let sessionHistory = [];
let fileBuffer = null;
let fileChunks = [];
let chunkIndex = 0;
let results = new Map();
let expectedTotalChunks = 0;
let taskStartTime = 0;
let totalFileSize = 0;
let processedBytes = 0;
let masterControlResult = null;

function broadcastToMaster(data) {
    const msg = JSON.stringify(data);
    for (const [ws, node] of nodes) {
        if (node.role === 'master' && ws.readyState === WebSocket.OPEN) {
            ws.send(msg);
        }
    }
}

function broadcastToAll(data) {
    const msg = JSON.stringify(data);
    for (const [ws, node] of nodes) {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(msg);
        }
    }
}

function sendToNode(ws, data) {
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function calculateNodeScore(node) {
    const gflops = node.gflops || 1;
    const latency = node.latency || 1000;
    const battery = node.battery !== undefined ? node.battery : 100;
    const cores = node.cores || 1;

    const latencyFactor = Math.max(0.1, 1 - (latency / 1000));
    const batteryFactor = Math.max(0.3, battery / 100);
    return (gflops * cores * latencyFactor * batteryFactor) / 100;
}

function fragmentFile(buffer, baseChunkSize) {
    const chunks = [];
    let offset = 0;
    let chunkId = 0;

    while (offset < buffer.length) {
        const remaining = buffer.length - offset;
        let chunkSize = Math.min(baseChunkSize, remaining);

        if (remaining > baseChunkSize && remaining < baseChunkSize * 1.5) {
            chunkSize = remaining;
        }

        const chunk = buffer.slice(offset, offset + chunkSize);
        chunks.push({
            id: chunkId++,
            data: chunk,
            start: offset,
            end: offset + chunkSize,
            size: chunk.length,
            assignedTo: null,
            status: 'pending',
            retries: 0
        });
        offset += chunkSize;
    }
    return chunks;
}

function assignChunks() {
    const availableNodes = Array.from(nodes.values())
        .filter(n => n.role === 'worker' && n.status === 'connected')
        .sort((a, b) => calculateNodeScore(b) - calculateNodeScore(a));

    if (availableNodes.length === 0) return;

    const pendingChunks = fileChunks.filter(c => c.status === 'pending' || (c.status === 'failed' && c.retries < 3));

    for (const chunk of pendingChunks) {
        if (availableNodes.length === 0) break;

        const scores = availableNodes.map(n => calculateNodeScore(n));
        const totalScore = scores.reduce((a, b) => a + b, 0);

        let selectedNode = null;
        let rand = Math.random() * totalScore;
        for (let i = 0; i < availableNodes.length; i++) {
            rand -= scores[i];
            if (rand <= 0) {
                selectedNode = availableNodes[i];
                break;
            }
        }
        selectedNode = selectedNode || availableNodes[0];

        const chunkSize = Math.round(chunk.size * (0.5 + calculateNodeScore(selectedNode) * 2));

        sendToNode(selectedNode.socket, {
            type: 'task',
            payload: {
                chunkId: chunk.id,
                data: chunk.data,
                operation: currentTask.operation,
                chunkSize: chunkSize
            }
        });

        chunk.status = 'processing';
        chunk.assignedTo = selectedNode.socket;
        chunk.startTime = Date.now();
        selectedNode.currentChunk = chunk.id;
        selectedNode.status = 'processing';
    }

    broadcastToMaster({ type: 'nodes_update', payload: getNodesInfo() });
}

function getNodesInfo() {
    const info = [];
    for (const [ws, node] of nodes) {
        info.push({
            id: node.id,
            name: node.name,
            role: node.role,
            os: node.os,
            browser: node.browser,
            cores: node.cores,
            ram: node.ram,
            battery: node.battery,
            latency: node.latency,
            downloadSpeed: node.downloadSpeed,
            gflops: node.gflops,
            score: calculateNodeScore(node),
            status: node.status,
            currentChunk: node.currentChunk,
            chunksDone: node.chunksDone || 0,
            processedMB: node.processedMB || 0,
            calcSpeed: node.calcSpeed || 0,
            activeThreads: node.activeThreads || 0
        });
    }
    return info;
}

function verifyIntegrity() {
    if (!masterControlResult) return { valid: true, message: 'No control result yet' };

    let totalCount = { A: 0, C: 0, G: 0, T: 0, N: 0 };
    for (const [chunkId, result] of results) {
        for (const base of Object.keys(totalCount)) {
            totalCount[base] += result.counts[base] || 0;
        }
    }

    const match = JSON.stringify(totalCount) === JSON.stringify(masterControlResult);
    return {
        valid: match,
        distributed: totalCount,
        master: masterControlResult,
        message: match ? 'Integridad verificada: resultados coinciden' : 'ERROR: Resultados no coinciden con control maestro'
    };
}

function completeTask() {
    const elapsed = (Date.now() - taskStartTime) / 1000;
    const speed = (totalFileSize / (1024 * 1024)) / elapsed;

    const integrity = verifyIntegrity();

    const sessionRecord = {
        timestamp: new Date().toISOString(),
        operation: currentTask.operation,
        fileName: currentTask.fileName,
        fileSize: totalFileSize,
        nodesCount: Array.from(nodes.values()).filter(n => n.role === 'worker').length,
        duration: elapsed,
        speedMBs: speed,
        totalChunks: expectedTotalChunks,
        integrity: integrity.valid,
        energyCost: calculateEnergyCost(elapsed)
    };

    sessionHistory.push(sessionRecord);

    broadcastToMaster({
        type: 'task_complete',
        payload: {
            ...sessionRecord,
            integrity,
            results: Object.fromEntries(results)
        }
    });

    currentTask = null;
    fileChunks = [];
    results.clear();
    chunkIndex = 0;
    masterControlResult = null;
}

function calculateEnergyCost(seconds) {
    const avgPhoneWatts = 3;
    const nodeCount = Array.from(nodes.values()).filter(n => n.role === 'worker').length;
    const kWh = (avgPhoneWatts * nodeCount * seconds) / (1000 * 3600);
    return (kWh * 0.10).toFixed(6);
}

function masterControlCount(buffer, operation) {
    if (operation !== 'count_bases') return null;
    const text = buffer.toString('utf-8').toUpperCase();
    const counts = { A: 0, C: 0, G: 0, T: 0, N: 0 };
    for (const char of text) {
        if (counts[char] !== undefined) counts[char]++;
        else counts.N++;
    }
    return counts;
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);

            switch (msg.type) {
                case 'register':
                    const nodeInfo = {
                        id: crypto.randomUUID().slice(0, 8),
                        name: msg.payload.name || `Node-${nodes.size + 1}`,
                        role: msg.payload.role || 'worker',
                        os: msg.payload.os || 'Unknown',
                        browser: msg.payload.browser || 'Unknown',
                        cores: msg.payload.cores || navigator?.hardwareConcurrency || 1,
                        ram: msg.payload.ram || 0,
                        battery: msg.payload.battery,
                        latency: 0,
                        downloadSpeed: 0,
                        gflops: msg.payload.gflops || 0,
                        status: 'connected',
                        socket: ws,
                        lastSeen: Date.now(),
                        chunksDone: 0,
                        processedMB: 0,
                        calcSpeed: 0,
                        activeThreads: 0
                    };
                    nodes.set(ws, nodeInfo);
                    sendToNode(ws, { type: 'registered', payload: { id: nodeInfo.id, role: nodeInfo.role } });
                    broadcastToMaster({ type: 'nodes_update', payload: getNodesInfo() });
                    console.log(`[${nodeInfo.role}] ${nodeInfo.name} conectado (${nodeInfo.cores} cores, ${nodeInfo.gflops} GFLOPS)`);
                    break;

                case 'heartbeat':
                    const node = nodes.get(ws);
                    if (node) {
                        node.lastSeen = Date.now();
                        node.latency = msg.payload.latency;
                        node.battery = msg.payload.battery;
                        node.downloadSpeed = msg.payload.downloadSpeed;
                        node.calcSpeed = msg.payload.calcSpeed;
                        node.activeThreads = msg.payload.activeThreads;
                        broadcastToMaster({ type: 'nodes_update', payload: getNodesInfo() });
                    }
                    break;

                case 'benchmark_result':
                    const benchNode = nodes.get(ws);
                    if (benchNode) {
                        benchNode.gflops = msg.payload.gflops;
                        console.log(`Benchmark ${benchNode.name}: ${msg.payload.gflops} GFLOPS`);
                    }
                    break;

                case 'chunk_result':
                    const resultNode = nodes.get(ws);
                    if (resultNode && currentTask) {
                        const chunkId = msg.payload.chunkId;
                        const chunk = fileChunks.find(c => c.id === chunkId);
                        if (chunk && chunk.status === 'processing') {
                            results.set(chunkId, msg.payload.result);
                            chunk.status = 'done';
                            chunk.endTime = Date.now();

                            resultNode.chunksDone = (resultNode.chunksDone || 0) + 1;
                            resultNode.processedMB = (resultNode.processedMB || 0) + (chunk.size / (1024 * 1024));
                            resultNode.status = 'connected';
                            resultNode.currentChunk = null;
                            processedBytes += chunk.size;

                            broadcastToMaster({
                                type: 'progress',
                                payload: {
                                    percent: (processedBytes / totalFileSize) * 100,
                                    speed: (processedBytes / (1024 * 1024)) / ((Date.now() - taskStartTime) / 1000),
                                    elapsed: (Date.now() - taskStartTime) / 1000,
                                    chunksDone: results.size,
                                    totalChunks: expectedTotalChunks,
                                    chunkMap: fileChunks.map(c => ({
                                        id: c.id,
                                        status: c.status,
                                        assignedTo: c.assignedTo ? nodes.get(c.assignedTo)?.name : null
                                    }))
                                }
                            });

                            if (results.size === expectedTotalChunks) {
                                completeTask();
                            } else {
                                assignChunks();
                            }
                        }
                    }
                    break;

                case 'start_task':
                    if (msg.payload.role !== 'master') break;
                    if (currentTask) {
                        sendToNode(ws, { type: 'error', payload: 'Ya hay una tarea en curso' });
                        break;
                    }

                    currentTask = {
                        operation: msg.payload.operation,
                        fileName: msg.payload.fileName,
                        chunkSize: msg.payload.chunkSize
                    };

                    const base64Data = msg.payload.fileData;
                    fileBuffer = Buffer.from(base64Data, 'base64');
                    totalFileSize = fileBuffer.length;
                    fileChunks = fragmentFile(fileBuffer, currentTask.chunkSize);
                    expectedTotalChunks = fileChunks.length;
                    chunkIndex = 0;
                    results.clear();
                    processedBytes = 0;
                    taskStartTime = Date.now();

                    masterControlResult = masterControlCount(fileBuffer, currentTask.operation);

                    broadcastToMaster({
                        type: 'task_started',
                        payload: {
                            operation: currentTask.operation,
                            fileName: currentTask.fileName,
                            totalChunks: expectedTotalChunks,
                            fileSize: totalFileSize
                        }
                    });

                    assignChunks();
                    break;

                case 'stop_task':
                    currentTask = null;
                    fileChunks.forEach(c => {
                        if (c.status === 'processing') c.status = 'pending';
                    });
                    broadcastToMaster({ type: 'task_stopped' });
                    break;

                case 'export_json':
                    const exportData = {
                        session: sessionHistory,
                        currentTask: currentTask ? {
                            operation: currentTask.operation,
                            fileName: currentTask.fileName,
                            progress: (processedBytes / totalFileSize) * 100,
                            results: Object.fromEntries(results),
                            integrity: verifyIntegrity()
                        } : null,
                        nodes: getNodesInfo(),
                        timestamp: new Date().toISOString()
                    };
                    sendToNode(ws, { type: 'export_result', payload: exportData });
                    break;

                case 'get_history':
                    sendToNode(ws, { type: 'history', payload: sessionHistory });
                    break;
            }
        } catch (e) {
            console.error('Error procesando mensaje:', e);
        }
    });

    ws.on('close', () => {
        const node = nodes.get(ws);
        if (node) {
            console.log(`Desconectado: ${node.name} (${node.role})`);
            if (node.role === 'worker' && currentTask && node.currentChunk) {
                const chunk = fileChunks.find(c => c.id === node.currentChunk);
                if (chunk) {
                    chunk.status = 'pending';
                    chunk.assignedTo = null;
                    chunk.retries = (chunk.retries || 0) + 1;
                    console.log(`Fragmento ${chunk.id} reencolado (reintento ${chunk.retries})`);
                    setTimeout(assignChunks, 1000);
                }
            }
        }
        nodes.delete(ws);
        broadcastToMaster({ type: 'nodes_update', payload: getNodesInfo() });
    });
});

setInterval(() => {
    for (const [ws, node] of nodes) {
        if (!ws.isAlive) {
            ws.terminate();
            continue;
        }
        ws.isAlive = false;
        ws.ping();
    }
}, HEARTBEAT_INTERVAL);

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`
╔══════════════════════════════════════════════════════════╗
║  FOG ORCHESTRATOR - SERVIDOR MAESTRO ACTIVO              ║
║  Puerto: ${PORT}                                             ║
║  Panel Maestro: http://localhost:${PORT}/master               ║
║  Nodo Móvil:    http://localhost:${PORT}/node                 ║
╚══════════════════════════════════════════════════════════╝
    `);
});