import shutil
import sys
from pathlib import Path

import pandas as pd

from src.ingestion.common import get_logger, landing_path

log = get_logger("hcp")


def run(local_file):
    src = Path(local_file)
    shutil.copy2(src, landing_path("hcp_original", src.name))  # untouched copy
    xls = pd.ExcelFile(src)
    for sheet in xls.sheet_names:
        df = xls.parse(sheet, header=None, dtype=str)  # no interpretation
        name = f"{src.stem}__{sheet}.csv".replace(" ", "_")
        df.to_csv(landing_path("hcp", name), index=False, header=False)
        log.info("exported sheet %s (%s rows)", sheet, len(df))


if __name__ == "__main__":
    run(sys.argv[1])
