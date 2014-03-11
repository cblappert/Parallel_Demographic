/*
 * Christopher Blappert and Michael Mitasev
 * 
 * Object that is used to pass a large amount of static data through the
 * calls of the ParallelBuildGrid object. Data is immutable once the 
 * object has been constructed.
 */
public class GridInfo {
	private int maxCols;
	private int maxRows;
	private float latGridSize;
	private float lonGridSize;
	private float minLat;
	private float minLon;
	
	// Post: constructs the object with the specified fields
	public GridInfo(int c, int r, float latGS, float lonGS, float lat, float lon) {
		maxCols = c;
		maxRows = r;
		latGridSize = latGS;
		lonGridSize = lonGS;
		minLat = lat;
		minLon = lon;
	}
	
	// Post: Getter method for the private field maxCols
	public int getMaxCols() {
		return maxCols;
	}
	
	// Post: Getter method for the private field maxRows
	public int getMaxRows() {
		return maxRows;
	}
	
	// Post: Getter method for the private field latGridSize
	public float getLatGridSize() {
		return latGridSize;
	}
	
	// Post: Getter method for the private field lonGridSize
	public float getLonGridSize() {
		return lonGridSize;
	}
	
	// Post: Getter method for the private field minLat
	public float getMinLat() {
		return minLat;
	}
	
	// Post: Getter method for the private field minLon
	public float getMinLon() {
		return minLon;
	}
}
