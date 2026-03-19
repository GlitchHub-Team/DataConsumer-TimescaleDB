# Data Consumer + TimescaleDB

repository MVP per il servizio di  Data Consumer

## Dipendenze
- NATS
- TimescaleDB (estensione PostgreSQL)

# Setup
## Creare .env file
Creare un file `.env` nella root del progetto e prendere il contenuto da `.env` su *Bitwarden*, nella cartella *DataConsumer*

## Creare server.key
Creare un file `server.key` nella cartella `services/nats/certs` prendere il contenuto da `server.key` su *Bitwarden*, nella cartella *DataConsumer*

## Creare admin_test.creds e data_consumer.creds
Creare i file `admin_test.creds` e `data_consumer.creds` nella cartella `cmd` prendere il contenuto da `admin_test.creds` e `data_consumer.creds` su *Bitwarden*, nella cartella *DataConsumer*