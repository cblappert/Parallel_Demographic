/*
 * Wrapper class for being able to return the maximum/minimum latitudes to determine the map
 * boundaries. Also keeps the total population. 
 */
public class PreprocessResult {
	public float highLat;
	public float lowLat;
	public float highLon;
	public float lowLon;
	public int totPop;
}
