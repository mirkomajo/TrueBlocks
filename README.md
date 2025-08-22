# TrueBlocks Modernized

This repository has been restructured into a modern application stack with a Python FastAPI backend and a retro-styled TypeScript dashboard frontend.

## Backend
- Located in `backend/` with main application in `backend/main.py`.
- Install dependencies: `pip install -r backend/requirements.txt`.
- Run development server: `uvicorn backend.main:app --reload`.

## Frontend
- Located in `frontend/` with source code under `frontend/src/`.
- Build the TypeScript sources using `npm install` and `npm run build` (requires network access to fetch dependencies).
- Open `frontend/public/index.html` in a browser to view the retro dashboard.

This scaffold provides a starting point for further development of the TrueBlocks project using a modern, professional code structure.
