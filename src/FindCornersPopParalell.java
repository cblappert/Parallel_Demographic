import java.util.concurrent.RecursiveTask;

/*
 * Christopher Blappert and Michael Mitasev
 * 
 * Helper class used to parallelize the preprocessing of the CensusData. Returns
 * a PreprocessResult object which contains the maximum and minimum latitude and
 * longitude of any CensusGroup in the CensusData. Also sums the population of the
 * CensusGroups.
 */

@SuppressWarnings("serial")
public class FindCornersPopParalell extends RecursiveTask<PreprocessResult> {
	private static final int CUTOFF_VALUE = 1000;
	private CensusData cData;
	private int start;
	private int end;

	// Pre: CensusData passed is not null, and has at least 1 element
	// Post: Initializes a new PreprocessTwo instance.
	public FindCornersPopParalell(CensusData data, int start, int end) {
		this.cData = data;
		this.start = start;
		this.end = end;
	}

	// Pre: N/A taken care of in constructor
	// Post: Finds the maximum and minimum latitude and longitude within the range specified
	// by start and end from the CensusData object. Also keeps a running sum of the
	// population in the given section of the CensusData object.
	@Override
	protected PreprocessResult compute() {
		PreprocessResult res = new PreprocessResult();
		if(end - start <= CUTOFF_VALUE) {
			res.highLat = cData.data[start].latitude;
			res.lowLat = cData.data[start].latitude;
			res.highLon = cData.data[start].longitude;
			res.lowLon = cData.data[start].longitude;
			for(int i = start; i < end; i++) {
				CensusGroup group = cData.data[i];
				res.totPop += group.population;
				res.highLon = Math.max(res.highLon, group.longitude);
				res.lowLon = Math.min(res.lowLon, group.longitude);
				res.highLat = Math.max(res.highLat, group.latitude);
				res.lowLat = Math.min(res.lowLat, group.latitude);
			}
		} else {
			FindCornersPopParalell right = new FindCornersPopParalell(cData, (start + end) / 2, end);
			FindCornersPopParalell left = new FindCornersPopParalell(cData, start, (start + end) / 2);
			right.fork();
			PreprocessResult leftRes = left.compute();
			PreprocessResult rightRes = right.join();
			res.highLat = Math.max(leftRes.highLat, rightRes.highLat);
			res.lowLat = Math.min(leftRes.lowLat, rightRes.lowLat);
			res.highLon = Math.max(leftRes.highLon, rightRes.highLon);
			res.lowLon = Math.min(leftRes.lowLon, rightRes.lowLon);
			res.totPop = leftRes.totPop + rightRes.totPop;
		}
		return res;
	}

}