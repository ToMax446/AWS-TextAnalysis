import java.util.Collection;
import java.util.List;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;

public class Parser {

    public static void parseFile(String fileName) {
        String parserModel = "edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz";
        LexicalizedParser lp = LexicalizedParser.loadModel(parserModel);

//        try (Stream<String> stream
//                     = Files.lines(Paths.get(fileName), StandardCharsets.UTF_8))
//        {
//            //run method for each line in stream lazily
//            stream.forEach(l -> DP(lp, l));
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }

        DP(lp, fileName);
    }

    // This option shows loading and using an explicit tokenizer
//    public static void API(LexicalizedParser lp, String line){
//        List<CoreLabel> rawWords = SentenceUtils.toCoreLabelList(line);
//        Tree parse = lp.apply(rawWords);
//        TokenizerFactory<CoreLabel> tokenizerFactory =
//                PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
//        Tokenizer<CoreLabel> tok =
//                tokenizerFactory.getTokenizer(new StringReader(line));
//        List<CoreLabel> rawWords2 = tok.tokenize();
//        parse = lp.apply(rawWords2);
//
//        TreebankLanguagePack tlp = lp.treebankLanguagePack(); // PennTreebankLanguagePack for English
//        GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
//        GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
//        List<TypedDependency> tdl = gs.typedDependenciesCCprocessed();
//        System.out.println(tdl);
//        System.out.println();
//    }

    public static void DP(LexicalizedParser lp, String filename) {
        // This option shows loading, sentence-segmenting and tokenizing
        // a file using DocumentPreprocessor.
        TreebankLanguagePack tlp = lp.treebankLanguagePack(); // a PennTreebankLanguagePack for English
        GrammaticalStructureFactory gsf = null;
        if (tlp.supportsGrammaticalStructures()) {
            gsf = tlp.grammaticalStructureFactory();
        }
        // You could also create a tokenizer here (as below) and pass it
        // to DocumentPreprocessor
        for (List<HasWord> sentence : new DocumentPreprocessor(filename)) {
            Tree parse = lp.apply(sentence);
            parse.pennPrint();
            System.out.println();

            if (gsf != null) {
                GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
                Collection tdl = gs.typedDependenciesCCprocessed();
                System.out.println(tdl);
                System.out.println();
            }
        }
    }
}
