
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# calcite-json-adapter

A dynamic reflective calcite adapter for json lists.

It contains calcite related classes to make Map<String, List < JSONObject> >

a SQL queryable schema, map keys are mapped to table names, each List  is mapper to a table,
and each JSONObject objec is mapped to row, JSONobject's keys are mapped as columns.


For more details about Calcite, see the [home page](http://calcite.apache.org).

calcite-json-adapter can only query the first level sub-objects of JSONObject, and only
sub-objects of the following Java classes can be mapped to columns,sub-objects of other JAVA type will be ignored.

String (mapped as VARCHAR)
Long (mapped as BIGINT)
Boolean (mapped as BOOLEAN)
Integer (mapped as INT)
BigDecimal (mapped as DECIMAL)
Double (mapped as FLOAT)
Float (mapped as REAL)
LocalDate (mapped as DATE)
LocalTime (mapped as TIME)
LocalDateTime (mapped as TIMESTAMP)


Sub-objects with the same key under different JSONObjects in the same List (table) must be the
same JAVA type, otherwise an IllegalArgumentException will be thrown when executing queries.

The DefaultMetadataProvider infers columns and their types from the data itself, it will merge all valid sub-object's keys to the full column list of the mapped table.

Users can write custom MetadataProviders to fetch columns and their data types from external storages.
