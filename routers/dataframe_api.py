import time
from fastapi import APIRouter, Request, HTTPException
from routers.analytics_api import record_execution

router = APIRouter(prefix="/api/patients", tags=["DataFrame"])


@router.get("/filter")
def filter_patients(request: Request, min_radius: float = 140.0, diag_type: str = "ICU"):
    try:
        start_time = time.time()
        mode = getattr(request.app.state, "execution_mode", "spark")

        # Kompatibilitas jika masih ada input 'M' atau 'B'
        dept_map = {"M": "ICU", "B": "ER"}
        target_dept = dept_map.get(diag_type.upper(), diag_type)

        if mode == "hadoop":
            df = getattr(request.app.state, "hadoop_patients_df", None)
            if df is None or df.empty:
                raise HTTPException(status_code=500, detail="Data pasien Hadoop belum tersedia.")

            mask = (df["department"] == target_dept) & (df["result_value"].astype(float) >= float(min_radius))
            filtered = df.loc[mask].head(500).copy()

            # Buat kolom alias agar langsung cocok dengan tabel di index.html
            filtered["id"] = filtered["patient_id"] + " (" + filtered["encounter_id"] + ")"
            filtered["diagnosis"] = filtered["department"] + " - " + filtered["diagnosis_code"] + " (" + filtered["test_name"] + ")"
            filtered["radius_mean"] = filtered["result_value"].astype(str) + " " + filtered["unit"]

            cols_to_return = ["id", "diagnosis", "radius_mean", "patient_id", "encounter_id", "department", "test_name", "result_value", "unit"]
            data = filtered[cols_to_return].fillna("").to_dict(orient="records")
        else:
            spark = getattr(request.app.state, "spark", None)
            if spark is None:
                raise HTTPException(status_code=500, detail="Spark Session belum diinisialisasi.")

            query = f"""
                SELECT 
                    CONCAT(patient_id, ' (', encounter_id, ')') AS id,
                    CONCAT(department, ' - ', diagnosis_code, ' (', test_name, ')') AS diagnosis,
                    CONCAT(CAST(result_value AS STRING), ' ', unit) AS radius_mean,
                    patient_id,
                    encounter_id,
                    department,
                    test_name,
                    result_value,
                    unit
                FROM patients
                WHERE department = '{target_dept}'
                  AND result_value >= {min_radius}
                LIMIT 500
            """
            result = spark.sql(query)
            df_pandas = result.toPandas().fillna("")
            data = df_pandas.to_dict(orient="records")

        exec_time = round(time.time() - start_time, 4)
        record_execution(request, "filter_data", exec_time)
        return {
            "mode": mode,
            "execution_time_sec": exec_time,
            "total_found": len(data),
            "data": data
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/all")
def get_all_patients(request: Request, limit: int = 100):
    try:
        mode = getattr(request.app.state, "execution_mode", "spark")

        if mode == "hadoop":
            df = getattr(request.app.state, "hadoop_patients_df", None)
            if df is None or df.empty:
                raise HTTPException(status_code=500, detail="Data pasien Hadoop belum tersedia.")

            columns = [c for c in df.columns if not str(c).startswith("Unnamed")]
            subset = df[columns].head(limit).fillna("")
            data = subset.to_dict(orient="records")
            return {"mode": mode, "columns": columns, "data": data}
        else:
            spark = getattr(request.app.state, "spark", None)
            if spark is None:
                raise HTTPException(status_code=500, detail="Spark Session belum diinisialisasi.")

            result = spark.sql(f"SELECT * FROM patients LIMIT {limit}")
            columns = [c for c in result.columns if not str(c).startswith("Unnamed")]
            df_pandas = result.select(*columns).toPandas().fillna("")
            data = df_pandas.to_dict(orient="records")
            return {"mode": mode, "columns": columns, "data": data}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))