
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


/**
 * メイン
 * Created by sekiguchikai on 2017/07/05.
 */
public class Main {
    // DoFnを実装したクラス
    // DoFnの横の<T,T>でinputとoutputの方の定義を行う
    static class FilterEvenFn extends DoFn<String, String> {
        // 実際の処理ロジックにはこのアノテーションをつける
        @ProcessElement
        // 実際の処理ロジックは、processElementメソッドに記述する
        // 引数のProcessContextを利用してinputやoutputを行う
        public void processElement(ProcessContext c) {
            System.out.print(c.element());
            // ProcessContextからinput elementを取得
            int num = Integer.parseInt(c.element());
            // input elementを使用した処理
            if (num % 2 == 0) {
                System.out.println("ifの結果" + num);
                // ProcessContextを使用して出力
                c.output(String.valueOf(num));
            }
        }
    }

    // インプットデータのパス
    private static final String INPUT_FILE_PATH = "./dataflow_number_test.csv";
    // アウトデータのパス
    private static final String OUTPUT_FILE_PATH = "./sample.csv";

    public static void main(String[] args) {
        // Optionを元にPipelineを生成する
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.create());

        // メソッドチェーンを使用
        pipeline.apply(TextIO.read().from(INPUT_FILE_PATH))
                .apply(ParDo.of(new FilterEvenFn()))
                // 書き込む
                .apply(TextIO.write().to(OUTPUT_FILE_PATH));

        // run : PipeLine optionで指定したRunnerで実行
        // waitUntilFinish : PipeLineが終了するまで待って、最終的な状態を返す
        pipeline.run().waitUntilFinish();
    }
}