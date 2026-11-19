# Fog Orchestrator — Computación en Niebla Distribuida

Sistema de orquestación web para procesamiento paralelo de archivos genómicos (u otros datasets grandes) distribuidos entre múltiples dispositivos móviles y una PC maestra vía red local/Wi-Fi.

## Características Principales

- **Arquitectura 100% en memoria** — Sin base de datos externa, exportación directa a JSON
- **Comunicación WebSocket bidireccional** — Baja latencia, heartbeats automáticos
- **Balanceo de carga adaptativo** — Asigna fragmentos según GFLOPS reales, latencia y batería
- **Tolerancia a fallos** — Reasignación automática de fragmentos si un nodo se desconecta
- **Procesamiento multi-hilo en móvil** — Web Workers usan todos los núcleos (`navigator.hardwareConcurrency`)
- **Wake Lock API** — Evita suspensión de pantalla en móviles durante procesamiento
- **Verificación de integridad** — Cruce de resultados distribuidos vs control maestro

## Estructura del Proyecto

```
fog-orchestrator/
├── server/
│   ├── server.js          # Servidor Node.js + WebSocket + Planificador
│   └── package.json
├── public/
│   ├── master/            # Panel de Control Maestro (Dashboard)
│   │   └── index.html     # Cyber-Dark/Neón, QR, métricas, mapa de bloques, terminal
│   └── node/              # Interfaz Nodo Periférico (Móvil)
│       ├── index.html     # Dark-Slate/Neón, estilo app nativa, anillo progreso circular
│       └── worker.js      # Web Worker: benchmark GFLOPS + procesamiento genómico
└── README.md
```

## Requisitos

- Node.js 18+
- Navegadores modernos con soporte para:
  - WebSockets
  - Web Workers
  - Wake Lock API (para mantener pantalla activa en móvil)
  - Battery Status API (opcional, para telemetría)
  - File API + TextDecoder

## Instalación y Ejecución Local

```bash
cd server
npm install
npm start
```

El servidor iniciará en `http://0.0.0.0:3000`

- **Panel Maestro:** `http://<IP_LOCAL>:3000/master`
- **Nodo Móvil:** `http://<IP_LOCAL>:3000/node`

## Despliegue en Producción (Ej: Render, Railway, Fly.io, VPS)

### Opción A: Render / Railway / Fly.io (Node.js Web Service)

1. Conecta tu repositorio GitHub
2. Build Command: `cd server && npm install`
3. Start Command: `cd server && npm start`
4. Puerto: `3000` (o el que asigne la plataforma via `process.env.PORT`)

> **Importante:** Los WebSockets requieren que la plataforma soporte WebSocket upgrade. Render, Railway y Fly.io lo soportan nativamente.

### Opción B: VPS (Ubuntu/Debian) con PM2 + Nginx

```bash
# En el servidor
git clone <tu-repo>
cd fog-orchestrator/server
npm install

# PM2 para proceso persistente
npm install -g pm2
pm2 start server.js --name fog-orchestrator
pm2 startup
pm2 save
```

**Nginx config (WebSocket proxy):**
```nginx
server {
    listen 80;
    server_name tu-dominio.com;

    location / {
        proxy_pass http://localhost:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }
}
```

## Uso

1. Abre el **Panel Maestro** en la PC
2. Escanea el código QR con los móviles (o abre la IP manualmente)
3. En cada móvil: escribe nombre → "Unirme a la niebla"
4. En el Panel: selecciona operación, archivo, tamaño de fragmento → **Iniciar**
5. Observa progreso en tiempo real, mapa de bloques, métricas por nodo
6. Al finalizar: **Exportar resultados a JSON**

## Operaciones Soportadas

| Operación | Descripción |
|-----------|-------------|
| `count_bases` | Conteo de bases A, C, G, T, N |
| `reverse_complement` | Complemento inverso de secuencia |
| `gc_content` | Porcentaje de contenido GC |
| `kmer_count` | Conteo de k-mers (k=4) |

## Algoritmo de Balanceo Adaptativo

El servidor calcula un **score** por nodo:
```
score = (GFLOPS × cores × factorLatencia × factorBatería) / 100
```

- `factorLatencia = max(0.1, 1 - latency/1000)`
- `factorBatería = max(0.3, battery/100)`

Los fragmentos se asignan probabilísticamente según score: nodos más potentes reciben fragmentos mayores.

## Tolerancia a Fallos

- Heartbeat cada 5s (ping/pong) para detectar latencia y desconexión
- Si nodo no responde en 30s → fragmento vuelve a cola `pending`
- Reintentos máximos: 3 por fragmento
- Verificación de integridad al finalizar (suma distribuida vs maestro)

## Exportación JSON

El archivo exportado contiene:
```json
{
  "session": [...historial...],
  "currentTask": { ... },
  "nodes": [...info nodos...],
  "timestamp": "ISO8601"
}
```

## Licencia

MIT — Libre para uso, modificación y distribución.