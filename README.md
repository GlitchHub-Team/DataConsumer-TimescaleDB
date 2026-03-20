# Data Consumer + TimescaleDB

repository MVP per il servizio di  Data Consumer

## Dipendenze
- NATS
- TimescaleDB (estensione PostgreSQL)

# Setup
## Clonare la repository Infrastructure
`git clone https://github.com/GlitchHub-Team/Infrastructure`

## Creare .env file
Creare un file `.env` nella cartella `Infrastructure` del progetto e prendere il contenuto da `.env` su *Bitwarden*, nella cartella *Infrastructure*

## Creare server.key
Creare un file `server.key` nella cartella `Infrastructure/nats/certs` prendere il contenuto da `server.key` su *Bitwarden*, nella cartella *Infrastructure*

## Creare stream_setupper.creds
Creare un file `stream_setupper.creds` nella cartella `Infrastructure/natsManager` prendere il contenuto da `stream_setupper.creds` su *Bitwarden*, nella cartella *Infrastructure*

## Creare admin_test.creds e data_consumer.creds
Creare i file `admin_test.creds` e `data_consumer.creds` nella cartella `cmd` prendere il contenuto da `admin_test.creds` e `data_consumer.creds` su *Bitwarden*, nella cartella *DataConsumer*

# Leggere Code Coverage
Seguire i seguenti passaggi per leggere la code coverage:
1. `chmod +x testCoverage.sh`
2. `./testCoverage.sh`
3. Aprire il file `coverage.html` con un browser per visualizzare la code coverage in modo interattivo.