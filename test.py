

def determine_geometry_type(coordinates: str) -> str:
    """
    Determine the geometry type based on the given coordinates.

    Args:
        coordinates (str): The coordinates.

    Returns:
        str: The geometry type.
    """
    if coordinates.startswith("[[[["):
        return "MultiPolygon"
    elif coordinates.startswith("[[["):
        return "Polygon"
    elif coordinates.startswith("[["):
        return "LineString"
    elif coordinates.startswith("["):
        return "Point"
    else:
        return None
    


# 創建一個 Polygon
poly = [[[0, 0], [1, 1], [1, 0], [0, 0]]]

# 創建一個 MultiPolygon
multi_poly = [[[[0, 0], [1, 1], [1, 0], [0, 0]]], [[[0, 0], [1, 1], [1, 0], [0, 0]]]]


print(determine_geometry_type(str(poly))) # Polygon