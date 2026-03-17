Here's the command to create and activate a Python virtual environment:

```bash
# Create the virtual env
python -m venv .venv

# Activate it — macOS/Linux
source .venv/bin/activate

# Activate it — Windows
.venv\Scripts\activate
```

Then once it's active, install your packages:

```bash
pip install pyspark delta-spark
```

To deactivate when you're done:

```bash
deactivate
```

The `.venv` folder name is just a convention — you can name it anything you like.

Once your virtual env is activated, just use `pip install` as normal:

```bash
# Install a single package
pip install pyspark

# Install multiple packages at once
pip install pyspark delta-spark jupyter

# Install a specific version
pip install pyspark==3.5.0
```

**The recommended workflow** is to maintain a `requirements.txt` file:

```bash
# Install everything from a requirements.txt
pip install -r requirements.txt

# Save your currently installed packages to requirements.txt
pip freeze > requirements.txt
```

A typical `requirements.txt` for PySpark + Delta Lake might look like:

```
pyspark==3.5.0
delta-spark==3.0.0
jupyter
pandas
pyarrow
```

A few useful pip commands to know:

```bash
pip list                  # see all installed packages
pip show pyspark          # details about a specific package
pip uninstall pyspark     # remove a package
pip install --upgrade pyspark  # upgrade to latest version
```

Always make sure your virtual env is **activated** before running pip — you'll see `(.venv)` at the start of your terminal prompt when it is.