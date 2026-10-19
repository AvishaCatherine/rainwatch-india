# RainWatch India — Setup on Windows 11 (8 GB RAM)

This project was developed and verified in a Linux environment. On Windows the classic failure
is:

```
HADOOP_HOME and hadoop.home.dir are unset
...
winutils.exe missing
```

That error comes from Spark's Hadoop I/O layer wanting native Windows binaries — it has nothing
to do with your project code. **Run `scripts/00_verify_spark.py` first**; it performs the minimal
test (Spark session → DataFrame → Parquet write → read → validate) and prints targeted advice if
anything is missing. Choose ONE of the options below.

---

## Option A (recommended): WSL Ubuntu — no winutils needed

You already have WSL Ubuntu, and the whole project (including Spark) runs identically inside it.

### One-time setup (paste into the Ubuntu terminal)

```bash
# 1. System prerequisites (Python 3.10+ is already in Ubuntu 22.04/24.04)
sudo apt update
sudo apt install -y python3-venv python3-pip openjdk-17-jre-headless

# 2. Project folder (if the ZIP is in Windows Downloads, it appears under /mnt/c)
cd ~
unzip /mnt/c/Users/Arul/Downloads/final.zip -d ~
cd rainwatch_india

# 3. Python virtual environment + dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. THE GATE TEST — must print SUCCESS before anything else
python scripts/00_verify_spark.py
```

Or simply run the helper script, which does steps 1-4:

```bash
bash scripts/setup_wsl.sh
```

### Memory settings for an 8 GB laptop

The defaults are already conservative (`spark.driver.memory=2g`, `local[*]` — WSL sees all
cores). If you also run a browser while the pipeline runs, lower the driver memory once:

```bash
export RAINWATCH_SPARK_DRIVER_MEM=1500m
```

(put it in `~/.bashrc` to persist). WSL itself: keep the default memory cap or add
`memory=6GB` to `%UserProfile%\.wslconfig` so Windows keeps breathing room.

### Daily use

```bash
cd ~/rainwatch_india && source .venv/bin/activate
python scripts/01_download_data.py      # once
python scripts/02_run_pipeline.py       # each time data changes
streamlit run dashboard/app.py          # the dashboard (opens in your Windows browser)
pytest -q                               # tests
```

---

## Option B: Native Windows (JDK + winutils.exe)

If you prefer not to use WSL:

1. **Install a JDK** (11, 17 or 21 — e.g. Eclipse Temurin 17).
   Set `JAVA_HOME` to the JDK folder and add `%JAVA_HOME%\bin` to `PATH`.

2. **Install Hadoop native binaries (winutils)** matching Hadoop 3.3.x:
   - Create the folder `C:\hadoop\bin`.
   - Download `winutils.exe` **and** `hadoop.dll` for Hadoop 3.3.6 from the widely used
     community repository (e.g. `github.com/cdarlint/winutils`, tag `hadoop-3.3.6/bin`).
     *(Always download binaries from a source you trust and scan them — this is exactly why
     Option A is recommended.)*
   - Copy both files into `C:\hadoop\bin`.

3. **Set environment variables** (System Properties → Environment Variables):
   ```
   JAVA_HOME   = C:\Program Files\Eclipse Adoptium\jdk-17...
   HADOOP_HOME = C:\hadoop
   PATH        += %HADOOP_HOME%\bin
   ```

4. **Verify the gate test**:
   ```powershell
   py -3.12 -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   python scripts\00_verify_spark.py
   ```
   `hadoop.dll` must sit next to `winutils.exe` in the same folder, or the Parquet write will
   still fail with `UnsatisfiedLinkError`.

5. If Parquet writes still complain, run once from an **administrator** PowerShell:
   `C:\hadoop\bin\winutils.exe chmod 777 C:\tmp` (creates/repairs the tmp permission cache).

### Known native-Windows caveats

- Antivirus/Defender can slow the many small Parquet files — add the project folder to
  exclusions if the pipeline feels sluggish.
- Long paths: keep the project under `C:\Projects\` rather than deep nested folders.

---

## If something still fails

`scripts/00_verify_spark.py` prints which prerequisite is missing (Java? HADOOP_HOME? winutils?)
and what to do. The pipeline has no other environment requirement — after the gate test prints
`RESULT: SUCCESS`, everything else is plain Python.
