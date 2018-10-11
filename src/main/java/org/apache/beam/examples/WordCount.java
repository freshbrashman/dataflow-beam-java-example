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
package org.apache.beam.examples;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Stream;


public class WordCount {

  private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

  /**
   * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
   * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
   * a ParDo in the pipeline.
   */
  static class ExtractWordsFn extends DoFn<String, String> {
    private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
    private final Distribution lineLenDist =
        Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
      lineLenDist.update(element.length());
      if (element.trim().isEmpty()) {
        emptyLines.inc();
      }

      // Split the line into words.
      String[] words = element.split(ExampleUtils.TOKENIZER_PATTERN, -1);

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(word);
        }
      }
    }
  }

  /** A SimpleFunction that converts a Word and Count into a printable string. */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  public static class CountWords
      extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

      return wordCounts;
    }
  }

  /**
   * Options supported by {@link WordCount}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
   * be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  public interface WordCountOptions extends AwsOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();

    void setOutput(String value);

    @Required
    String getTableName();
    void setTableName(String value);

  }

  static void runWordCount(WordCountOptions options) {
    Pipeline p = Pipeline.create(options);

    TableFieldSchema [] fields = {
            new TableFieldSchema().setName("col1").setType("STRING"),
            new TableFieldSchema().setName("col2").setType("INTEGER"),
            new TableFieldSchema().setName("col3").setType("INTEGER"),
            new TableFieldSchema().setName("col4").setType("INTEGER")
    };
    TableSchema tableSchema = new TableSchema().setFields(Arrays.asList(fields));


    // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    p
      .apply("ReadLines", TextIO.read().from(options.getInputFile()))
      .apply(ParDo.of(new DoFn<String, TableRow>() {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<TableRow> receiver) {
          JSONObject elementJson = new JSONObject(element);
          String [] columns = {"col1", "col2", "col3", "col4"};
          TableRow tableRow = new TableRow();

          Arrays.stream(columns).forEach((column) -> {
            switch(column) {
              case "col1":
                try{
                  String valStr = elementJson.getString(column);
                  tableRow.put(column, valStr);
                }catch(Exception e) {
                }
                break;
              default:
                try{
                  int valInt = elementJson.getInt(column);
                  tableRow.put(column, valInt);
                }catch(Exception e) {
                }
            }
            // 意味もなく負荷をかける
            JSONObject dummy = new JSONObject(element);
            for(int i=0; i<10; i++) {
                LOG.info(dummy.toString());
                JSONObject dummy2 = new JSONObject(element);
                dummy2.put("element", dummy);
                dummy = dummy2;
                LOG.info(dummy.toString());
            }
          });

          receiver.output(tableRow);
        }
      }))
      .apply("WriteBigQuery",
              BigQueryIO.writeTableRows()
                      .to("test02." + options.getTableName())
                      .withSchema(tableSchema)
                      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                      .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                      );
//      .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    p.run();
//    p.run().waitUntilFinish();  // 同期実行の場合
  }

  public static void main(String[] args) {

    String uniqueKey = "testxxxxxxxxyyyyxzz";

    String [] args2 = {
            "--runner=DataflowRunner",
            "--project=sylvan-overview-145200",
            String.format("--gcpTempLocation=gs://yterui-dataflow-test/dataflow-apache-example/gcpTempLocation/%s/", uniqueKey),
            String.format("--tempLocation=gs://yterui-dataflow-test/dataflow-apache-example/tempLocation/%s/", uniqueKey),
            String.format("--stagingLocation=gs://yterui-dataflow-test/dataflow-apache-example/staging/%s/", uniqueKey),
//              "--inputFile=gs://apache-beam-samples/shakespeare/*",
//            "--inputFile=gs://yterui-function-test/bq_load_test/bq_load_test_3.json.gz",
            "--inputFile=s3://yterui-test-bucket-01/dataflow-input/*",
            "--tableName=" + uniqueKey,
            String.format("--output=gs://yterui-dataflow-test/dataflow-apache-example/output/%s/", uniqueKey),
            "--awsRegion=us-east-2",
            "--network=vpc-test",
            "--subnetwork=subnet-test"
    };

    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args2).withValidation().as(WordCountOptions.class);

    options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(new BasicAWSCredentials(System.getenv("accesskey"), System.getenv("secretkey"))));


    runWordCount(options);
  }

/*
  // https://qiita.com/Keisuke69/items/23ce6652f212a7418fac
  public static String handler(S3Event event, Context context){
    LambdaLogger lambdaLogger = context.getLogger();

    event.getRecords().stream().parallel().forEach((record) -> {
      lambdaLogger.log(record.getEventName()); //イベント名
      lambdaLogger.log(record.getS3().getBucket().getName()); //バケット名
      lambdaLogger.log(record.getS3().getObject().getKey()); //オブジェクトのキー（オブジェクト名）

      String uniqueKey = record.getS3().getObject().getKey().replaceAll("[\\/\\.\\(\\)\\-]", "");

      String [] args = {
              "--runner=DataflowRunner",
              "--project=sylvan-overview-145200",
              String.format("--gcpTempLocation=gs://yterui-dataflow-test/dataflow-apache-example/gcpTempLocation/%s/", uniqueKey),
              String.format("--tempLocation=gs://yterui-dataflow-test/dataflow-apache-example/tempLocation/%s/", uniqueKey),
              String.format("--stagingLocation=gs://yterui-dataflow-test/dataflow-apache-example/staging/%s/", uniqueKey),
//              "--inputFile=gs://apache-beam-samples/shakespeare/*",
              "--inputFile=gs://yterui-function-test/bq_load_test/bq_load_test_3.json.gz",
              "--tableName=" + uniqueKey,
              String.format("--output=gs://yterui-dataflow-test/dataflow-apache-example/output/%s/", uniqueKey)
      };

      main(args);
    });

    return "Success!!";
  }
*/
}
