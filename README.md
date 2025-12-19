# Railway Delay Archiver — How to Run

This project collects, stores, and enriches real-time railway delay data in France using the SNCF / Navitia public API.

---

## 1. Get an SNCF / Navitia API token

1. Create an account at:  
   https://www.navitia.io
2. Generate an API token
3. Create a `.env` file at the root of the project with the following content:

```bash
SNCF_TOKEN=your_api_token_here
```

## 2. Clone the repository
```bash
git clone https://github.com/zhukovanadezhda/railway-delay-archiver.git
cd railway-delay-archiver
```

## 3. Create the Conda environment
```bash
conda create -r requirements.yml
conda activate railway-delay-archiver
```

## 4. Run the data pipeline
```bash
bash main.sh [create_csv]
```
Use the flag if you want to export the final joint csv file.

This command will automatically:
- create the SQLite database if it does not exist
- fetch and store all railway stations
- periodically collect real-time train departure data
- aggregate raw observations into train-level records
- enrich the data with calendar and weather information

The pipeline runs continuously and updates the database every hour, if you want to stop it use `Ctrl+C`.

