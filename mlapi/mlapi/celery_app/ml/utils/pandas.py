def duplicate_pandas_column(pandas_df, column, new_name=None, inplace=False):
    """Duplicates pandas column.

    Parameters
    ----------
    pandas_df : pd.DataFrame

    column : str
        Column to duplicate

    new_name : str, default=None
        Name of new duplicated column. If None, the same as `column` is used.

    inplace : bool, default=False
    """
    if new_name is None:
        new_name = column

    if inplace:
        # If inplace is True, change same input `pandas_df`
        pandas_df[new_name] = pandas_df[column].copy()
    else:
        pandas_df = pandas_df.copy()
        pandas_df[new_name] = pandas_df[column].copy()
        return pandas_df
