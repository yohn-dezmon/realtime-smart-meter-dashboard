import geohash2
import geojson
from geojson import Point, Feature, GeometryCollection, FeatureCollection
import pandas as pd

class GeoJSON(object):
    """ A class to create GeoJSON schema for the lattitude and longitude coordinates
    created from the geohashes in my dataset.
    """

    def main(self):
        pass

    def createGeoJSON(self, df):
        geoJSONList = []
        # converts geohash to GPS tuple of type str
        df['GPS'] = df['geohash'].apply(lambda x : geohash2.decode(x))
        # converts GPS str tuple to type float
        df['GPS'] = df['GPS'].apply(lambda x : tuple(map(float, x)))
        insert_features = lambda X: geoJSONList.append(
                Feature(geometry=Point(X['GPS']),
                                properties=dict(id=X['geohash'],
                                )))
        df.apply(insert_features, axis=1)
        
        return geoJSONList


if __name__ == '__main__':
    # testing createGeoJSON
    data = ['gcpuwq9nq1kz','gcpuwqc7ymcv','gcpuwqcmq71c']
    df = pd.DataFrame(data, columns = ['geohash'])

    geoJSON = GeoJSON()
    geoJSON.createGeoJSON(df)
