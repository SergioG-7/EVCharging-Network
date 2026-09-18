# ⚡ EVCharging Network - Distributed Management System

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://www.mysql.com/)
[![OpenWeather](https://img.shields.io/badge/OpenWeather_API-EB6E4B?style=for-the-badge&logo=openweathermap&logoColor=white)](https://openweathermap.org/)

Sistema distribuido resiliente y en tiempo real para la gestión, monitorización y seguridad de una red de puntos de recarga de vehículos eléctricos (Charging Points / CP).

El proyecto implementa principios de **Arquitectura Orientada a Servicios (SOA)**, streaming de eventos asíncrono, comunicación por sockets TCP, APIs RESTful y mecanismos de seguridad de canal (SSL/TLS con certificados) junto con cifrado de carga útil a nivel de aplicación.

---

## 🏗️ Arquitectura del Sistema

El sistema opera desacoplado en nodos independientes preparados para ejecutarse tanto en red local distribuida (multi-PC) como mediante contenedores Docker:

```
                                  [ OpenWeather API ]
                                           ▲
                                           │ HTTPS (Rest)
                                           ▼
[ Browser ] ──HTTP──> [ Front (Web) ] ──REST──> [ EV_Central (Core + BD) ] ◄──TLS/Rest── [ EV_Registry ]
                                                       ▲           ▲                             ▲
                                                       │           │                             │
                                         Streaming QM  │           │ Sockets (Salud/Auth)        │ Altas/Bajas
                                        (Apache Kafka) │           │ (Simétrico dinámico)        │ (Canal Seguro)
                                                       ▼           ▼                             │
                                              [ EV_CP_Engine ] ◄───┼─────────────────────────────┘
                                                       ▲           │
                                                Sockets│           ▼
                                                       └── [ EV_CP_Monitor ]
                                                                   ▲
[ EV_Driver (Simulador/Lotes) ] ──Streaming (Kafka) ───────────────┘
```

### Componentes Principales

*   **`EV_Central` (Core System):** Orquestador central del sistema. Gestiona el estado operativo de los CPs y usuarios, base de datos de persistencia (`database.py`), registro de auditoría de eventos en tiempo real y revocación dinámica de credenciales.
*   **`API_Central` & `Front.py`:** Dashboard web público y servidor API REST que expone el estado de los cargadores (disponible, suministrando, fuera de servicio, averiado), telemetría de consumos e información meteorológica en tiempo real.
*   **`EV_CP` (Charging Points):**
    *   **`EV_CP_M` (Monitor):** Módulo supervisor del cargador. Gestiona el alta y baja ante `EV_Registry`, autenticación con la Central y health checks periódicos por sockets con el motor[cite: 4].
    *   **`EV_CP_E` (Engine):** Controlador de suministro energético. Emite telemetría de carga cifrada hacia la Central mediante tópicos de Apache Kafka.
*   **`EV_W` (Weather Control Office):** Microservicio que consulta periódicamente (cada 4s) la temperatura de cada ubicación a través de la API de OpenWeather. Notifica a `EV_Central` para ordenar paradas preventivas de emergencia si la temperatura cae por debajo de 0 °C.
*   **`EV_Registry`:** Servicio de autorización y registro de nuevos CPs. Gestiona el ciclo de vida del punto de carga mediante endpoints REST y canal seguro cifrado mediante certificados SSL (`certServ.pem`).
*   **`EV_Driver`:** Aplicación de clientes/conductores. Permite solicitar suministros puntuales o en lotes desatendidos leyendo archivos de transacciones (`recargas_cp.txt`) a través del broker de eventos.

---

## 🔒 Mecanismos de Seguridad y Resiliencia

*   **Canal Seguro SSL/TLS:** Autenticación e intercambio inicial de credenciales de cargadores con `EV_Registry` bajo canales cifrados con certificado PEM (`certServ.pem`).
*   **Cifrado Simétrico Dinámico:** Generación de claves de cifrado simétricas únicas por CP tras la autenticación. Todos los payloads de telemetría de recarga depositados en Kafka van encriptados para evitar ataques Man-in-the-Middle (MITM).
*   **Revocación de Claves:** Capacidad desde la Central de invalidar la sesión de un CP de forma remota ante incidencias o brechas, forzando la reautenticación.
*   **Auditoría Estructurada:** Registro pormenorizado en base de datos de cada acción (IP de origen, timestamp, acción, parámetros y estado).
*   **Tolerancia a Fallos y Reconexión:** Manejo de interrupciones de red, reintentos automáticos en colas Kafka y finalización segura de carga ante averías o congelación.

---

## 📁 Estructura del Repositorio

```text
.
├── CENTRAL/
│   ├── API_Central.py        # Endpoints REST para monitorización y alertas meteorológicas
│   ├── chargingpoint.py      # Modelo y lógica de negocio de los puntos de recarga
│   ├── compose.yml           # Despliegue de Central, Base de Datos y Kafka
│   ├── creadorbd.sql         # Script DDL para inicialización del esquema de BD
│   ├── database.py           # Capa de abstracción y persistencia (MySQL)
│   ├── Dockerfile            # Imagen contenerizada del nodo Central
│   ├── EV_Central.py         # Lógica central del sistema distribuido
│   ├── Front.py              # Interfaz web de monitorización
│   ├── lanzar_Central.py     # Script orquestador de arranque local
│   └── requirements.txt      # Dependencias del nodo Central
├── CP/
│   ├── compose.yml           # Despliegue multi-contenedor para estaciones de recarga
│   ├── Dockerfile            # Imagen contenerizada del entorno CP
│   ├── EV_CP_E.py            # Engine: gestión de hardware, métricas y streaming
│   ├── EV_CP_M.py            # Monitor: health checks, registro y autenticación
│   ├── EV_W.py               # Weather Office: cliente de OpenWeather API
│   ├── lanzar_CP.py          # Script de inicialización de cargadores
│   ├── lanzar_W.py           # Script de arranque del servicio de clima
│   ├── requirements.txt      # Dependencias del entorno CP
│   └── weather_config.txt    # Configuración de ciudades y coordenadas asignadas
└── DRIVER/
    ├── certServ.pem          # Certificado digital para conexiones seguras SSL
    ├── compose.yml           # Despliegue contenerizado de clientes y registro
    ├── Dockerfile            # Imagen contenerizada de simuladores
    ├── EV_Driver.py          # Simulador de conductor (interacción con Kafka)
    ├── EV_Registry.py        # Microservicio de registro y expedición de tokens
    ├── lanzar_Driver.py      # Script de lanzamiento masivo de conductores
    ├── lanzar_Registry.py    # Script de arranque del Registry
    ├── recargas_cp.txt       # Cola de cargas por lotes preprogramadas
    └── requirements.txt      # Dependencias de clientes y registro
```

---

## 🚀 Despliegue y Ejecución

El sistema está preparado para desplegarse en **3 máquinas independientes** en red local (o en una máquina única utilizando diferentes puertos):

### Requisitos Previos

*   Python 3.10+
*   Docker y Docker Compose
*   Broker Apache Kafka
*   API Key de [OpenWeatherMap](https://openweathermap.org/)

---

### Escenario Multi-Máquina (3 PCs)

#### Máquina 1: Core System (Central, Front & Kafka)
1. Iniciar servicios base (Kafka, Zookeeper, MySQL):
   ```bash
   cd CENTRAL
   docker compose up -d
   ```
2. Ejecutar backend y monitorización:
   ```bash
   python lanzar_Central.py
   ```

#### Máquina 2: Charging Points & Weather Office
1. Configurar la IP de la Máquina 1 en `weather_config.txt` y en los scripts de red.
2. Iniciar el monitor climático:
   ```bash
   cd CP
   python lanzar_W.py
   ```
3. Iniciar puntos de recarga (Engine + Monitor):
   ```bash
   python lanzar_CP.py
   ```

#### Máquina 3: Drivers & Registry Service
1. Levantar el microservicio de registro seguro:
   ```bash
   cd DRIVER
   python lanzar_Registry.py
   ```
2. Lanzar la simulación de conductores:
   ```bash
   python lanzar_Driver.py
   ```

---

## 📊 Dashboard y Telemetría

El panel web es accesible vía navegador en `http://<IP_CENTRAL>:<PUERTO_FRONT>` y muestra:

*   **Identificación y Ubicación:** Cargadores activos por identificador y dirección.
*   **Estado Operativo:** Activado (Verde), Suministrando (Verde con métricas en kW y €), Parado / Alerta climática (Naranja) y Averiado (Rojo).
*   **Telemetría del Clima:** Monitorización térmica en tiempo real provista por `EV_W`.
*   **Transacciones Activas:** ID de conductor, consumo instantáneo y tarificación calculada en vivo.
