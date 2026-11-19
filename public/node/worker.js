// Web Worker para procesamiento pesado en segundo plano
// Ejecuta benchmark GFLOPS y procesamiento de fragmentos genómicos

let coreCount = navigator.hardwareConcurrency || 1;
let isProcessing = false;

// ===== BENCHMARK GFLOPS =====
function runBenchmark(cores) {
    const matrixSize = 128; // 128x128 matrices
    const iterations = 3;
    const float64Array = new Float64Array(matrixSize * matrixSize);

    // Initialize matrices with random data
    const a = new Float64Array(matrixSize * matrixSize);
    const b = new Float64Array(matrixSize * matrixSize);
    const c = new Float64Array(matrixSize * matrixSize);

    for (let i = 0; i < a.length; i++) {
        a[i] = Math.random();
        b[i] = Math.random();
    }

    function matmul(m, n, k, A, B, C) {
        for (let i = 0; i < m; i++) {
            for (let j = 0; j < n; j++) {
                let sum = 0;
                for (let l = 0; l < k; l++) {
                    sum += A[i * k + l] * B[l * n + j];
                }
                C[i * n + j] = sum;
            }
        }
    }

    let totalFlops = 0;
    const startTotal = performance.now();

    for (let iter = 0; iter < iterations; iter++) {
        const start = performance.now();
        matmul(matrixSize, matrixSize, matrixSize, a, b, c);
        const elapsed = performance.now() - start;
        const flops = (2 * matrixSize * matrixSize * matrixSize) / (elapsed / 1000);
        totalFlops += flops;
    }

    const avgGflops = (totalFlops / iterations) / 1e9;
    return { gflops: avgGflops.toFixed(2), cores };
}

// ===== PROCESAMIENTO DE FRAGMENTOS =====
function processChunk(chunk, data, operation) {
    const startTime = performance.now();
    const text = new TextDecoder('utf-8').decode(data);
    const upperText = text.toUpperCase();

    let result = null;

    switch (operation) {
        case 'count_bases':
            result = countBases(upperText);
            break;
        case 'reverse_complement':
            result = reverseComplement(upperText);
            break;
        case 'gc_content':
            result = gcContent(upperText);
            break;
        case 'kmer_count':
            result = countKmers(upperText, 4);
            break;
        default:
            result = { error: 'Operación desconocida' };
    }

    const elapsed = performance.now() - startTime;
    const sizeMB = data.byteLength / (1024 * 1024);
    const speedMBs = sizeMB / (elapsed / 1000);

    return {
        result,
        stats: {
            sizeMB,
            speedMBs,
            elapsedMs: elapsed,
            threads: coreCount
        }
    };
}

function countBases(text) {
    const counts = { A: 0, C: 0, G: 0, T: 0, N: 0 };
    for (let i = 0; i < text.length; i++) {
        const char = text[i];
        if (counts[char] !== undefined) counts[char]++;
        else if (char !== '\n' && char !== '\r' && char !== ' ') counts.N++;
    }
    return { counts, total: text.length };
}

function reverseComplement(text) {
    const complement = { A: 'T', T: 'A', C: 'G', G: 'C', N: 'N' };
    let result = '';
    for (let i = text.length - 1; i >= 0; i--) {
        const char = text[i];
        if (complement[char]) result += complement[char];
        else if (char !== '\n' && char !== '\r' && char !== ' ') result += 'N';
    }
    return { sequence: result, length: result.length };
}

function gcContent(text) {
    let gc = 0, at = 0, other = 0;
    for (let i = 0; i < text.length; i++) {
        const c = text[i];
        if (c === 'G' || c === 'C') gc++;
        else if (c === 'A' || c === 'T') at++;
        else if (c !== '\n' && c !== '\r' && c !== ' ') other++;
    }
    const total = gc + at;
    return { gc, at, other, gcPercent: total > 0 ? (gc / total * 100).toFixed(2) : 0 };
}

function countKmers(text, k) {
    const kmers = new Map();
    let valid = 0;
    for (let i = 0; i <= text.length - k; i++) {
        const kmer = text.slice(i, i + k);
        if (/^[ACGT]+$/.test(kmer)) {
            kmers.set(kmer, (kmers.get(kmer) || 0) + 1);
            valid++;
        }
    }
    // Return top 20 kmers
    const sorted = Array.from(kmers.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 20);
    return { kmers: Object.fromEntries(sorted), totalValid: valid, unique: kmers.size };
}

// ===== MESSAGE HANDLER =====
self.onmessage = function(e) {
    const msg = e.data;

    if (msg.type === 'benchmark') {
        coreCount = msg.cores;
        const bench = runBenchmark(coreCount);
        self.postMessage({ type: 'benchmark_result', gflops: bench.gflops, cores: bench.cores });
        return;
    }

    if (msg.type === 'process') {
        if (isProcessing) {
            self.postMessage({ type: 'error', message: 'Worker ya está procesando' });
            return;
        }
        isProcessing = true;

        const { chunk, data, operation } = msg;
        const arrayBuffer = data;
        const uint8Array = new Uint8Array(arrayBuffer);

        // Simulate progress updates for large chunks
        if (uint8Array.length > 500000) {
            const steps = 10;
            const stepSize = Math.ceil(uint8Array.length / steps);
            for (let i = 0; i < steps; i++) {
                const progress = ((i + 1) / steps) * 100;
                self.postMessage({ type: 'progress', percent: progress, suffix: `${Math.round(progress)}%` });
                // Small delay to allow UI updates
                const start = performance.now();
                while (performance.now() - start < 10) {}
            }
        }

        try {
            const { result, stats } = processChunk(chunk, uint8Array, operation);
            self.postMessage({ type: 'chunk_complete', result, stats });
        } catch (err) {
            self.postMessage({ type: 'error', message: err.message });
        } finally {
            isProcessing = false;
        }
    }
};

// Notify ready
self.postMessage({ type: 'ready', cores: coreCount });