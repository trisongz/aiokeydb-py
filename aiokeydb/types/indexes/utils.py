
def check_numeric(x) -> bool:
    """
    Check if a value is numeric
    """
    if isinstance(x, (int, float, complex)): return True
    if isinstance(x, str):
        try:
            float(x)
            return True
        except Exception as e:
            return False
