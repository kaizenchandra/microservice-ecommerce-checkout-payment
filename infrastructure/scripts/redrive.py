#!/usr/bin/env python3
"""Run the Kafka recovery tool using dependencies from a locally built service jar."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile


def main():
    if len(sys.argv) not in (5, 6):
        raise SystemExit("Usage: redrive.py BOOTSTRAP DLT_TOPIC PARTITION OFFSET [--execute]")
    root = Path(__file__).resolve().parents[2]
    jar = root / "order-query-service/target/order-query-service-1.0.0-SNAPSHOT.jar"
    if not jar.exists():
        raise SystemExit("Build order-query-service first: mvn -pl order-query-service -am package -DskipTests")
    with tempfile.TemporaryDirectory(prefix="kafka-redrive-") as directory, zipfile.ZipFile(jar) as archive:
        libraries = []
        for prefix in ("BOOT-INF/lib/kafka-clients-", "BOOT-INF/lib/slf4j-api-"):
            candidates = [name for name in archive.namelist() if name.startswith(prefix) and name.endswith(".jar")]
            if len(candidates) != 1:
                raise SystemExit("Expected Kafka and SLF4J runtime dependencies in the service jar")
            target = Path(directory) / Path(candidates[0]).name
            target.write_bytes(archive.read(candidates[0]))
            libraries.append(str(target))
        java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else "java"
        result = subprocess.run([java, "--class-path", os.pathsep.join(libraries),
                                 str(root / "infrastructure/scripts/Redrive.java"), *sys.argv[1:]], timeout=60)
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
