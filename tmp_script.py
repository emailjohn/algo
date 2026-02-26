from algo.data.universe import get_clean_universe

# Giv mig alle kvalitets-aktier og etf'er:
assets = get_clean_universe(kinds={"equity", "etf"}, min_coverage=0.5)
print("Clean Universe:", assets)
