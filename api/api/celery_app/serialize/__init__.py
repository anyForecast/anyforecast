"""DataFrame serializers.

This package contains classes that implement input serialization
for DataFrame-like objects.

These classes essentially take user input, a model object that
represents what the expected input should look like, and it returns
a dictionary that contains the various parts of a request.  A few
high level design decisions:

* Each protocol type maps to a separate class, all inherit from
  ``Serializer``.

* The return value for ``serialize_to_request`` (the main entry
  point) returns a dictionary that represents a request.  This
  will have keys like ``url_path``, ``query_string``, etc.  This
  is done so that it's a) easy to test and b) not tied to a
  particular HTTP library.  See the ``serialize_to_request`` docstring
  for more details.
"""