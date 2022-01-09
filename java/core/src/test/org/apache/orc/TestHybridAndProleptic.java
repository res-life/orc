/**
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
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;

/**
 * Test ColumnStatisticsImpl for ORC.
 */
public class TestHybridAndProleptic {

  @Test
  public void readHybridByProleptic() throws IOException {
    Configuration conf = new Configuration();
    // convert to proleptic calendar
    conf.set("orc.proleptic.gregorian", "true");
    Reader reader = OrcFile.createReader(new Path("/home/chong/read-hybrid-as-proleptic.orc"),
        OrcFile.readerOptions(conf));
    System.out.println("File schema: " + reader.getSchema());
    System.out.println("File row count: " + reader.getNumberOfRows());

    Date dateForFilter = Date.valueOf("1582-10-03");
    System.out.println("Filter is c1 == " + dateForFilter);

    RecordReader rowIterator = reader.rows(
        reader.options()
            .searchArgument(SearchArgumentFactory.newBuilder()
                .equals("c1", PredicateLeaf.Type.DATE, dateForFilter)
                .build(), new String[]{"c1"}) //predict push down
    );

    // Read the row data
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    DateColumnVector x = (DateColumnVector) batch.cols[0];

    System.out.println("-------------find-------------------------");
    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        int xRow = x.isRepeating ? 0 : row;
        System.out.println("c1: " + (x.noNulls || !x.isNull[xRow] ?
            x.vector[xRow] :null));
      }
    }
    rowIterator.close();
  }
}
