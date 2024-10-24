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

package FlinkCommerce;

import Deserializer.JSONValueDeserializationSchema;
import Dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;

public class DataStreamJob {
	private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
	private static final String username = "postgres";
	private static final String password = "postgres";

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = "sales_transactions";

		KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
				.setBootstrapServers("localhost:9092")
				.setTopics(topic)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();

		DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

		transactionStream.print();

		JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();

		JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(jdbcUrl)
				.withDriverName("org.postgresql.Driver")
				.withUsername(username)
				.withPassword(password)
				.build();


		//create transactions table
		transactionStream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_transactions (" +
						"id INTEGER PRIMARY KEY, " +
						"uid VARCHAR(255), " +
						"color VARCHAR(255), " +
						"department VARCHAR(255), " +
						"material VARCHAR(255), " +
						"product_name VARCHAR(255), " +
						"price DOUBLE PRECISION, " +
						"price_string VARCHAR(255), " +
						"promo_code VARCHAR(255) " +
						")",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {

				},
				execOptions,
				connOptions
		)).name("Create Transactions Table Sink");

		transactionStream.addSink(JdbcSink.sink(
				"INSERT INTO sales_transactions(id, uid, color, department, material, " +
						"product_name, price, price_string, promo_code) " +
						"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
						"ON CONFLICT (id) DO UPDATE SET " +
						"id = EXCLUDED.id, " +
						"uid  = EXCLUDED.uid, " +
						"color  = EXCLUDED.color, " +
						"department = EXCLUDED.department, " +
						"material = EXCLUDED.material, " +
						"product_name = EXCLUDED.product_name, " +
						"price  = EXCLUDED.price, " +
						"price_string = EXCLUDED.price_string, " +
						"promo_code  = EXCLUDED.promo_code " +
						"WHERE sales_transactions.id = EXCLUDED.id",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
					preparedStatement.setInt(1, transaction.getId());
					preparedStatement.setString(2, transaction.getUid());
					preparedStatement.setString(3, transaction.getColor());
					preparedStatement.setString(4, transaction.getDepartment());
					preparedStatement.setString(5, transaction.getMaterial());
					preparedStatement.setString(6, transaction.getProduct_name());
					preparedStatement.setDouble(7, transaction.getPrice());
					preparedStatement.setString(8, transaction.getPrice_string());
					preparedStatement.setString(9, transaction.getPromo_code());
				},
				execOptions,
				connOptions
		)).name("Insert into transactions table sink");

		// Execute program, beginning computation.
		env.execute("Flink Ecommerce Realtime Streaming");
	}
}
