package co.empathy.academy.imdb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.regex.Pattern;

public class TSVMerger {

    private static final Pattern DELIMITER = Pattern.compile("\t");
    private static final Logger logger = LoggerFactory.getLogger(TSVMerger.class);


    public static void cleanAkas(String filmsPath, String akasPath, String outputPath) throws IOException {
        try (
                BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputPath));
                BufferedReader filmsReader = new BufferedReader(new FileReader(filmsPath));
                BufferedReader akasReader = new BufferedReader(new FileReader(akasPath));
        ) {
            String filmsLine = filmsReader.readLine();
            String akasLine = akasReader.readLine();
            String[] firstAkasLineArray = {"titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"};
            for (String s : firstAkasLineArray) {
                outputWriter.write(s);
                outputWriter.write("\\t");
            }
            outputWriter.newLine();
            filmsLine = filmsReader.readLine();
            akasLine = akasReader.readLine();

            while (filmsLine != null && akasLine != null) {
                var filmLineArray = filmsLine.split(DELIMITER.pattern());
                var akasLineArray = akasLine.split(DELIMITER.pattern());

                var filmIndex = Integer.valueOf(filmLineArray[0].split("tt")[1]);
                var akasIndex = Integer.valueOf(akasLineArray[0].split("tt")[1]);

                var filmIndexCorrected = filmLineArray[0];

                while (filmIndex == akasIndex) {
                    outputWriter.write(akasLine);
                    akasLine = akasReader.readLine();
                    akasLineArray = akasLine.split(DELIMITER.pattern());
                    akasIndex = Integer.valueOf(akasLineArray[0].split("tt")[1]);
                }

                if (filmIndex != akasIndex) {
                    if (filmIndex < akasIndex) {
                        outputWriter.write(filmIndexCorrected);
                        outputWriter.write("\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N");
                        outputWriter.newLine();
                        filmsLine = filmsReader.readLine();
                    } else if (filmIndex > akasIndex) {
                        akasLine = akasReader.readLine();
                    }
                }

            }

        }
    }
}




















