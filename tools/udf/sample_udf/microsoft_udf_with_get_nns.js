/**
 * Copyright (c) Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function HandleRequest(executionMetadata, ...input) {
  let keyGroupOutputs = [];
  for (let argument of input) {
    let keyGroupOutput = {};
    keyGroupOutput.tags = argument.tags;
    let data = argument.hasOwnProperty('tags') ? argument.data : argument;
    const getNearestNeighborsResult = JSON.parse(getNearestNeighbors(data));
    // getNearestNeighborsResult returns "kvPairs" when successful and "code" on failure.
    // Ignore failures and only add successful getNearestNeighborsResult lookups to output.
    if (getNearestNeighborsResult.hasOwnProperty('kvPairs')) {
      const kvPairs = getNearestNeighborsResult.kvPairs;
      const keyValuesOutput = {};
      for (const key in kvPairs) {
        if (kvPairs[key].hasOwnProperty('keysetValues')) {
          // kvPairs[key].keysetValues.values is an array of neighbor embeddings for each key embedding.
          // values inside sorted by proximity to the key embedding.
          // The number of values returned is determined by index itself.
          keyValuesOutput[key] = { value: kvPairs[key].keysetValues.values };
        } else {
          keyValuesOutput[key] = { status: kvPairs[key].status };
        }
      }
      keyGroupOutput.keyValues = keyValuesOutput;
      keyGroupOutputs.push(keyGroupOutput);
    }
  }
  return { keyGroupOutputs, udfOutputApiVersion: 1 };
}
