package fourth

import groovy.util.logging.Slf4j

@Slf4j
class WebBarRateGenerator {

    def SQUARE = 2
    def generateWebBAR(List<Double> currentRate)
    {
        log.info "=====> $currentRate"
        if (currentRate.size() == 0 ) return 0.0;
        def mean = meanOf(currentRate);
        double stddev = standardDeviationOf(currentRate,mean)
        (mean - stddev) > 0.0  ? mean : medianOf(currentRate)
    }


    private medianOf(List values)
    {
        values.sort();
        (values.size() % 2 == 0
                ? values.get((int)values.size()/2)+values.get((int)(values.size()/2)+1)/2
                : values.get((int)(values.size()/2)+1)
        )
    }
    private standardDeviationOf(List values,double mean)
    {
        List stdDev = new ArrayList<Double>();
        for (currVal in values){
            stdDev.add(Math.pow(currVal - mean,SQUARE))
        }
        return Math.sqrt(stdDev.sum())
    }
    private meanOf(List values) {
        def mean = 0.0;
        for (competitorRate in values) {
            mean += competitorRate;
        }
        return mean/values.size()
    }

}
