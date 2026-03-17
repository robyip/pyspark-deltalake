PySpark requires Java to run. Here's how to fix it:

**1. Install Java**
```bash
sudo apt install default-jdk -y    # Ubuntu/Debian
```

**2. Find your Java installation path**
```bash
java -version          # confirm it's installed
which java             # find the binary
readlink -f $(which java)  # get the full real path
```

**3. Set JAVA_HOME**

The path is usually `/usr/lib/jvm/java-11-openjdk-amd64` or similar. Set it permanently by adding it to your `~/.bashrc`:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

**4. Verify it's set**
```bash
echo $JAVA_HOME
java -version
```

**5. Run your script again** — the error should be gone.

> Note: PySpark works best with **Java 11** or **Java 17**. If `readlink` gave you a different path in step 2, use that path instead for `JAVA_HOME`.