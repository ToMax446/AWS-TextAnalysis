package tools;

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
        FileInputStream fstream = new FileInputStream(fileLocation);
        DataInputStream in = new DataInputStream(fstream); // Get the object of DataInputStream
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        StringReader sr; // we need to re-read each line into its own reader because the tokenizer is over-complicated garbage
        PTBTokenizer tkzr; // tokenizer object
        WordStemmer ls = new WordStemmer(); // stemmer/lemmatizer object

        // Read File Line By Line
        String strLine;
        ArrayList<String> words = new ArrayList();
        ArrayList<String> tags = new ArrayList();
        String penn = null;
        StringBuilder result = new StringBuilder();

        Collection tdl = null;
        while ((strLine = br.readLine()) != null) {
            System.out.println("Tokenizing and Parsing: " + strLine); // print current line to console

            // do all the standard java over-complication to use the stanford parser tokenizer
            sr = new StringReader(strLine);
            tkzr = PTBTokenizer.newPTBTokenizer(sr);
            List toks = tkzr.tokenize();
            System.out.println("tokens: " + toks);

            Tree parse = (Tree) lp.apply(toks); // finally, we actually get to parse something

            // Get words, stemmed words and POS tags

            if (type.equals("POS")) {
                for (TaggedWord tw : parse.taggedYield()) {
                    words.add(tw.word());
                    tags.add(tw.tag());
                    result.append(tw);
                }
            }
            if (type.equals("DEPENDENCY")) {
                TreebankLanguagePack tlp = new PennTreebankLanguagePack();
                GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
                GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
                tdl = gs.typedDependenciesCollapsed();
                result.append(tdl);
            }

            if (type.equals("CONSTITUENCY")) {
                penn = parse.pennString();
                result.append(penn);
            }
        }
        File file = new File("output.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.append(result);
        }
        return file;
    }
}