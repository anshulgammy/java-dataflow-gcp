package blog.utopian.nerd;

import static blog.utopian.nerd.util.RedditPostBodyAnalyzerUtil.getRedditPostBodyList;

import blog.utopian.nerd.model.RedditPostBodyOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Dataflow driver class to load the csv file, and analyze which words are most used in Reddit post
 * section. Output is written to csv file, and also printed on the console.
 */
// Note:
// Command to run this dataflow in local is: mvn exec:java \
//    -Dexec.mainClass=blog.utopian.nerd.RedditPostBodyAnalyzerDataflow \
//    -Dexec.args="--output=/Users/anshulgautam/Downloads/output-file"
public class RedditPostBodyAnalyzerDataflow {

  public static void main(String[] args) throws IOException {

    // Create Pipeline Options
    RedditPostBodyOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(RedditPostBodyOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read CSV File", Create.of(getRedditPostBodyList(options.getInputFile())))

        // Extract words from post line.
        .apply(
            "Extract Words",
            FlatMapElements.into(TypeDescriptors.strings())
                .via(
                    (String line) ->
                        Arrays.asList(
                            line.replace("\n", "")
                                .replace("\r", "")
                                .trim()
                                .toLowerCase()
                                .split(" "))))
        .apply("Filter Empty Words", Filter.by((String word) -> !word.isEmpty()))

        // Count each word
        .apply("Count Words", Count.perElement())

        // Convert KV to string for sorting
        .apply(
            "KV to String",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                .via((KV<String, Long> kv) -> KV.of(kv.getKey(), kv.getValue())))

        // Put into iterable and sort descending
        .apply("Global Combine", Combine.globally(new SortByValueDescending()).withoutDefaults())

        // Flatten sorted list
        .apply("Flatten", Flatten.iterables())

        // Print results
        .apply(
            "Prepare Output",
            ParDo.of(
                new DoFn<KV<String, Long>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctx) {
                    KV<String, Long> kv = ctx.element();
                    String preparedString = (kv.getKey() + " : " + kv.getValue());
                    System.out.println(preparedString);
                    ctx.output(preparedString);
                  }
                }))
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));

    pipeline.run().waitUntilFinish();
  }

  // Custom CombineFn to sort word count by value descending
  static class SortByValueDescending
      extends Combine.CombineFn<
          KV<String, Long>, List<KV<String, Long>>, Iterable<KV<String, Long>>> {
    @Override
    public List<KV<String, Long>> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<KV<String, Long>> addInput(List<KV<String, Long>> acc, KV<String, Long> input) {
      acc.add(input);
      return acc;
    }

    @Override
    public List<KV<String, Long>> mergeAccumulators(Iterable<List<KV<String, Long>>> accs) {
      List<KV<String, Long>> merged = new ArrayList<>();
      for (List<KV<String, Long>> acc : accs) {
        merged.addAll(acc);
      }
      return merged;
    }

    @Override
    public Iterable<KV<String, Long>> extractOutput(List<KV<String, Long>> acc) {
      acc.sort(Comparator.comparingLong(KV<String, Long>::getValue).reversed());
      return acc;
    }
  }
}
