function truthScoreToLevel(truthScore) {
    truthScore *= 100;
    // Constants for what thresholds... (Assume the truth score ranges from 0 to 100.)
    const MAXIMUM_SCORE = 100;
    const TRUTH_THRESHOLD = 90;
    const QUESTIONABLE_THRESHOLD = 50;
    const MINIMUM_SCORE = 0;

    // "Switch" statement...
    if(truthScore >= TRUTH_THRESHOLD)
        return 1;
    else if(truthScore >= QUESTIONABLE_THRESHOLD)
        return 2;
    else if(truthScore >= MINIMUM_SCORE)
        return 3;
    else
        return 0;

    // 1 is truthful, 2 is questionable (somewhat true), and 3 is false.
    // 0 signals not to put anything on the banner if some error occurred.
}