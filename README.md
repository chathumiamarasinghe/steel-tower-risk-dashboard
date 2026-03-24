# Steel Tower Risk

Steel Tower Risk is a demo web application that maps U.S. transmission steel towers with an XGBoost-derived exposure score and related storm and DOE outage context. A FastAPI backend loads a scored CSV into memory at startup (no database), and a React + MapLibre GL frontend renders all towers as a single GeoJSON layer for smooth panning and filtering at scale.

## Local development

1. **Place the dataset**  
   Copy `master_final_scored.csv` into `backend/data/` so the path is:

   `backend/data/master_final_scored.csv`

2. **Run the API** (from the `backend` folder):

   ```bash
   cd backend
   pip install -r requirements.txt
   uvicorn main:app --reload --port 8000
   ```

3. **Run the frontend** (from the `frontend` folder):

   ```bash
   cd frontend
   npm install
   npm run dev
   ```

4. Open the Vite dev server URL (usually `http://localhost:5173`). Set `VITE_API_URL` in `frontend/.env` if the API is not at `http://localhost:8000`.


## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health payload |
| GET | `/health` | Health check |
| GET | `/towers/stats` | Totals by concern color, average score, top owners |
| GET | `/towers/geojson` | Filtered GeoJSON (`color`, `volt_class`, `owner`, `min_score`, `max_score`) |
| GET | `/towers/filters/options` | Voltage classes, color list, NERC regions |
| GET | `/towers/{tower_id}` | Single tower by `id` (use URL encoding for `/` in ids) |

## Data source credits

- **HIFLD** — transmission facility locations and attributes  
- **NOAA Storm Events** — historical storm events  
- **DOE OE-417** — electric emergency incidents and customer impacts  
- **NARR** — North American Regional Reanalysis (meteorological context)
