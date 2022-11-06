from sklearn.utils.validation import check_is_fitted


class FeaturesSegmenter:
    """Segments transformed features based on the passed schema.

    Parameters
    ----------
    preprocessor : sklearn Pipeline
        Fitted preprocessor.

    schema : SchemaResolver
        Instance of :class:`SchemaResolver`.

    steps_to_ignore : list of str, default=None

    """
    def __init__(
            self, preprocessor, schema, steps_to_ignore=None,
            ignore_remainder=True, timestamp_type_to='time_varying_known',
            drop_timestamp=True
    ):
        check_is_fitted(preprocessor)
        self.preprocessor = preprocessor
        self.schema = schema
        if steps_to_ignore is None:
            steps_to_ignore = []
        self.steps_to_ignore = steps_to_ignore
        self.ignore_remainder = ignore_remainder
        self.timestamp_type_to = timestamp_type_to
        self.drop_timestamp = drop_timestamp

    def segment(self):
        names_for_all = self.schema.get_names_for('all')
        for name, step in self.preprocessor.named_steps.items():
            if name in self.steps_to_ignore:
                continue

            for name, trans, features_in, _ in step.iter():
                if name == 'remainder' and self.ignore_remainder:
                    continue

                features_in = self._get_features_in(features_in, trans)
                features_out = self._get_features_out(trans)

                # (feature_type, names) tuple where ``feature_type`` is one
                # of time_varying_known, time_varying_unknown, etc.,
                # and ``names`` a list of feature names belonging to such type.
                for feature_type, names in names_for_all.items():
                    if self._check_multi_membership(names, features_in):
                        names = self._bulk_remove(names, features_in)
                        names = self._bulk_insert(names, features_out)
                        if feature_type == 'timestamp':
                            names_for_all[self.timestamp_type_to].extend(names)

                        else:
                            names_for_all[feature_type] = names

                        break

        if self.drop_timestamp:
            names_for_all.pop('timestamp')

        return names_for_all

    def _get_features_out(self, trans):
        """Obtains features out from transformer.
        """
        return trans.get_feature_names_out()

    def _get_features_in(self, features_in, trans):
        """Tries to obtain ``features_in`` directly from transformer.
        If not possible returns the one from meth:`step.iter()`.
        """
        try:
            features_in = trans.feature_names_in_
        except AttributeError:
            if not isinstance(features_in, list):
                features_in = [features_in]
        return features_in

    def _check_multi_membership(self, ls, items):
        """Checks the membership of multiple values at once in a list
        """
        return all(x in ls for x in items)

    def _bulk_remove(self, ls, to_remove):
        """Removes multiple items from a list.
        """
        return [x for x in ls if x not in to_remove]

    def _bulk_insert(self, ls, to_insert):
        """Removes multiple items from a list.
        """
        return ls + list(to_insert)

    def _is_name_to_ignore(self, name):
        return (name == 'remainder' and self.ignore_remainder) or (
                name in self.steps_to_ignore)
