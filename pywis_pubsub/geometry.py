###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

from shapely import geometry


def is_message_within_bbox(message_geometry: dict, bbox_filter: list) -> bool:
    """
    Assesses whether one geometry is within another

    :param message geometry: GeoJSON geometry to test
    :param bbox_filter: geometry of bbox filter (minx, miny, maxx, maxy)

    :returns: `bool` of assessment result
    """

    geometry1 = geometry.shape(message_geometry)
    geometry2 = geometry.box(*bbox_filter)

    return geometry1.within(geometry2)
