def tabulate(data, headers=None, tablefmt="grid"):
    """
    Simplified version of tabulate for formatting tabular data.
    Supports 'grid' table format only.
    """
    if not data:
        return "No data available."

    # Calculate column widths
    columns = headers if headers else list(range(len(data[0])))
    col_widths = [len(str(col)) for col in columns]

    for row in data:
        col_widths = [max(len(str(cell)), col_widths[i]) for i, cell in enumerate(row)]

    # Build table
    horizontal_line = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
    header_line = (
        "|" + "|".join([f" {str(col).ljust(w)} " for col, w in zip(columns, col_widths)]) + "|"
        if headers
        else ""
    )

    rows = "\n".join(
        "|" + "|".join([f" {str(cell).ljust(w)} " for cell, w in zip(row, col_widths)]) + "|" for row in data
    )

    # Combine parts
    table = horizontal_line + "\n"
    if headers:
        table += header_line + "\n" + horizontal_line + "\n"
    table += rows + "\n" + horizontal_line

    return table
