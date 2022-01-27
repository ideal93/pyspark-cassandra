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

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.spark.connector.{toRDDFunctions, GettableData}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.datastax.oss.protocol.internal.ProtocolConstants

case class DataFrame(names: Array[String], types: Array[String], values: Seq[ArrayBuffer[Any]])

object SpanBy {
  def binary(rdd: RDD[UnreadRow], columns: Array[String]) = {
    // span by the given columns
    val spanned = rdd.spanBy { r => columns.map { c => r.row.getBytesUnsafe(c) } }

    // deserialize the spans
    spanned.map {
      case (k, rows) => {
        // get the columns for the data frame (so excluding the ones spanned by)
        val colDefs = rows.head.row.getColumnDefinitions.asScala
        val colTypesWithIdx = colDefs.zipWithIndex.filter {
          case (c, i) => columns.contains(c.getName)
        }

        // deserialize the spanning key
        val deserializedKey = k.zipWithIndex.map {
          case (bb, i) => GettableData.get(rows.head.row, i)
        }

        // transpose the rows in to columns and 'deserialize'
        val df = colDefs.map { _ => new ArrayBuffer[Any] }.toArray
        for {
          row <- rows
          (ct, i) <- colTypesWithIdx
        } {
          df(i) += deserialize(row, ct.getType, i)
        }

        // list the numpy types of the columns in the span (i.e. the non-key columns)
        val numpyTypes = colTypesWithIdx.map { case (c, i) => numpyType(c.getType).getOrElse(null) }

        // return the key and 'dataframe container'
        (deserializedKey, new DataFrame(columns, numpyTypes.toArray, df.toSeq))
      }
    }
  }

  /**
    * 'Deserializes' a value of the given type _only if_ there is no binary representation possibly which can
    * be converted into a numpy array. I.e. longs will _not_ actually be deserialized, but Strings or UUIDs
    * will. If possible the value will be written out as a binary string for an entire column to be converted
    * to Numpy arrays.
    */
  private def deserialize(row: UnreadRow, dt: DataType, i: Int) = {
    if (binarySupport(dt)) {
      row.row.getBytesUnsafe(i)
    } else {
      GettableData.get(row.row, i)
    }
  }

  /** Checks if a Cassandra type can be represented as a binary string. */
  private def binarySupport(dataType: DataType) = {
    numpyType(dataType) match {
      case Some(x) => true
      case None => false
    }
  }

  /** Provides a Numpy type string for every Cassandra type supported. */
  private def numpyType(dataType: DataType) = {
    Option(dataType.getProtocolCode match {
      case ProtocolConstants.DataType.BOOLEAN => ">b1"
      case ProtocolConstants.DataType.INT => ">i4"
      case ProtocolConstants.DataType.BIGINT => ">i8"
      case ProtocolConstants.DataType.COUNTER => ">i8"
      case ProtocolConstants.DataType.FLOAT => ">f4"
      case ProtocolConstants.DataType.DOUBLE => ">f8"
      case ProtocolConstants.DataType.TIMESTAMP => ">M8[ms]"
      case _ => null
    })
  }
}
