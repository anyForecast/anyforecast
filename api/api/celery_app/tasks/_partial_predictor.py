from ._transformers import WhatIf


class PartialPredictor:

    def __init__(
            self, X, timestamp_col, input_cols,
            predictor, date_range
    ):
        self.X = X
        self.timestamp_col = timestamp_col
        self.input_cols = input_cols
        self.predictor = predictor
        self.date_range = date_range

    def predict(self, *args, **kwargs):
        what_if = self._make_what_if(*args)
        X = what_if.fit_transform(self.X)
        return self.predictor.predict(X, **kwargs)

    def _make_what_if(self, *args):
        value = {self.input_cols[i]: arg for i, arg in enumerate(args)}
        print(value)
        return WhatIf(
            self.date_range,
            self.timestamp_col,
            self.input_cols,
            value=value
        )
