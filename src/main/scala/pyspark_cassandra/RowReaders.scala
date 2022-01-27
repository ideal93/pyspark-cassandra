/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pyspark_cassandra

import com.datastax.oss.driver.api.core.cql.{Row => DriverRow}
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.rdd.reader.{RowReader, RowReaderFactory}
import com.datastax.spark.connector.{CassandraRowMetadata, ColumnRef, GettableData}

/** A container for a 'raw' row from the java driver, to be deserialized. */
case class UnreadRow(row: DriverRow, columnNames: Array[String], table: TableDef) {
  def deserialize(c: String) = {
    if (row.isNull(c)) null else GettableData.get(row, c)
  }

  def deserialize(c: Int) = {
    if (row.isNull(c)) null else GettableData.get(row, c)
  }
}

class DeferringRowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef])
  extends RowReader[UnreadRow] {

  def targetClass = classOf[UnreadRow]

  override def neededColumns: Option[Seq[ColumnRef]] = None // TODO or selected columns?

  override def read(row: DriverRow, rowMetaData: CassandraRowMetadata): UnreadRow = {
    assert(row.getColumnDefinitions().size() >= rowMetaData.columnNames.length, "Not enough columns available in row")
    UnreadRow(row, rowMetaData.columnNames.toArray, table)
  }
}

class DeferringRowReaderFactory extends RowReaderFactory[UnreadRow] {
  def targetClass: Class[UnreadRow] = classOf[UnreadRow]

  def rowReader(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]): RowReader[UnreadRow] = {
    new DeferringRowReader(table, selectedColumns)
  }
}
