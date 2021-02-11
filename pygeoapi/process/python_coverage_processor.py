# =================================================================
#
# Authors: Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (C) 2021 EOX IT Services GmbH <https://eox.at>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================


from pygeoapi.process.base import BaseProcessor


PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "python-coverage-processor",
    "title": "Python Coverage Processor",
    "description": "",
    "keywords": [],
    "links": [],
    "inputs": [
        {
            "id": "data",
            "title": "data",
            "abstract": "Collections to be retrieved",
            "input": {"formats": [{"default": True, "mimeType": "application/json"}]},
            "minOccurs": 1,
            "maxOccurs": "unbounded",
            "metadata": None,  # TODO how to use?
            "keywords": [],
        },
        {
            "id": "sourceBands",
            "title": "source bands",
            "abstract": "",
            "input": {"formats": [{"default": True, "mimeType": "application/json"}]},
            "minOccurs": 1,
            "maxOccurs": "unbounded",
            "metadata": None,
            "keywords": [],
        },
        {
            "id": "bandsPythonFunctions",
            "title": "Python functions to be applied on bands",
            "abstract": "",
            "input": {"formats": [{"default": True, "mimeType": "application/json"}]},
            "minOccurs": 1,
            "maxOccurs": "unbounded",
            "metadata": None,
            "keywords": [],
        },
    ],
    "outputs": [
        {
            "id": "result",
            "title": "Result of the calculation on the data",
            "description": "",
            "output": {"formats": [{"mimeType": "image/tiff; application=GeoTIFF"}]},
        }
    ],
    "example": {},
}


class PythonCoverageProcessor(BaseProcessor):
    """
    NOTE: this is a notebook being run for coverage processing,
          so we can't execute this directly. However we need this here for documentation.

    """

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)