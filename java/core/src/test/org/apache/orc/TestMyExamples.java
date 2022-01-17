/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class TestMyExamples {
  @Test
  public void testCreateOrcFile() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");
    File file = new File("/tmp/my-file.orc");
    if (file.exists()) {
      file.delete();
    }

    int stride = 1000;
    int rows = stride * 3;
    Path fsPath = new Path(file.getAbsolutePath());
    try (Writer writer = OrcFile.createWriter(fsPath,
        OrcFile.writerOptions(new Configuration())
            .rowIndexStride(stride)
            .compress(CompressionKind.NONE)
            .setSchema(schema))) {
      VectorizedRowBatch batch = schema.createRowBatch(rows);
      LongColumnVector x = (LongColumnVector) batch.cols[0];
      BytesColumnVector y = (BytesColumnVector) batch.cols[1];

      for (int r = 0; r < rows; ++r) {
        int row = batch.size++;
        x.vector[row] = r;
        if (row % 2 == 0) {
          x.isNull[row] = true;
          x.noNulls = false;
        }
        byte[] buffer = ("s" + r).getBytes(StandardCharsets.UTF_8);
        y.setRef(row, buffer, 0, buffer.length);
        if (row % 2 == 1) {
          y.isNull[row] = true;
          y.noNulls = false;
        }
        // If the batch is full, write it out and start over.
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      }

      if (batch.size != 0) {
        writer.addRowBatch(batch);
      }
    }
  }
}
