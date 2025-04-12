package blog.utopian.nerd.model;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface RedditPostBodyOptions extends PipelineOptions {

  @Description("Path of the file to read from")
  @Default.String("/Users/anshulgautam/Downloads/input-reddit-data/high_school.csv")
  String getInput();

  void setInput(String value);

  /** Set this required option to specify where to write the output. */
  @Description("Path of the file to write to")
  @Validation.Required
  String getOutput();

  void setOutput(String value);
}
