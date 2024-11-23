import json
from typing import Dict, List, Union, Optional, Callable
from xml.dom import minidom
import xml.etree.ElementTree as ET
from dataclasses import dataclass
import logging

@dataclass
class OsmElement:
    id: int
    type: str
    tags: Dict[str, str]
    nodes: List[int]  # For ways
    members: List[Dict]  # For relations
    lat: Optional[float] = None  # For nodes
    lon: Optional[float] = None  # For nodes

class OsmToGeoJson:
    def __init__(self, enhanced_properties: bool = False, verbose: bool = False):
        """
        Initialize the OSM to GeoJSON converter.
        
        Args:
            enhanced_properties (bool): If True, include more structured properties in output
            verbose (bool): If True, output diagnostic information during processing
        """
        self.enhanced_properties = enhanced_properties
        self.logger = logging.getLogger(__name__)
        if verbose:
            logging.basicConfig(level=logging.INFO)
        else:
            logging.basicConfig(level=logging.WARNING)
        
    def convert(self, input_data: Union[str, Dict], callback: Optional[Callable] = None) -> Dict:
        """
        Convert OSM data to GeoJSON.
        
        Args:
            input_data: Either XML string or JSON dict of OSM data
            callback: Optional callback function for streaming output
            
        Returns:
            Dict containing GeoJSON FeatureCollection
        """
        if isinstance(input_data, str):
            if input_data.lstrip().startswith('{'):
                data = json.loads(input_data)
            else:
                data = self._parse_xml(input_data)
        else:
            data = input_data
            
        self.logger.info("Parsing OSM data...")
        elements = self._parse_elements(data)
        
        if callback:
            return self._convert_streaming(elements, callback)
        else:
            return self._convert_batch(elements)
    
    def _parse_xml(self, xml_str: str) -> Dict:
        """Parse OSM XML format into internal dictionary format."""
        self.logger.info("Parsing XML input...")
        root = ET.fromstring(xml_str)
        
        data = {
            "version": float(root.attrib.get("version", 0.6)),
            "elements": []
        }
        
        for elem in root:
            if elem.tag in ('node', 'way', 'relation'):
                element = {
                    "type": elem.tag,
                    "id": int(elem.attrib["id"])
                }
                
                if elem.tag == 'node':
                    element["lat"] = float(elem.attrib["lat"])
                    element["lon"] = float(elem.attrib["lon"])
                
                elif elem.tag == 'way':
                    element["nodes"] = [
                        int(nd.attrib["ref"]) 
                        for nd in elem.findall("nd")
                    ]
                
                elif elem.tag == 'relation':
                    element["members"] = [
                        {
                            "type": member.attrib["type"],
                            "ref": int(member.attrib["ref"]),
                            "role": member.attrib["role"]
                        }
                        for member in elem.findall("member")
                    ]
                
                # Parse tags for all element types
                element["tags"] = {
                    tag.attrib["k"]: tag.attrib["v"]
                    for tag in elem.findall("tag")
                }
                
                data["elements"].append(element)
        
        return data
    
    def _parse_elements(self, data: Dict) -> Dict[str, Dict[int, OsmElement]]:
        """Convert raw OSM data into structured internal format."""
        elements = {
            "nodes": {},
            "ways": {},
            "relations": {}
        }
        
        for elem in data["elements"]:
            parsed = OsmElement(
                id=elem["id"],
                type=elem["type"],
                tags=elem.get("tags", {}),
                nodes=elem.get("nodes", []),
                members=elem.get("members", []),
                lat=elem.get("lat"),
                lon=elem.get("lon")
            )
            
            if elem["type"] == "node":
                elements["nodes"][elem["id"]] = parsed
            elif elem["type"] == "way":
                elements["ways"][elem["id"]] = parsed
            elif elem["type"] == "relation":
                elements["relations"][elem["id"]] = parsed
                
        return elements
    
    def _convert_batch(self, elements: Dict) -> Dict:
        """Convert parsed OSM elements to GeoJSON FeatureCollection."""
        features = []
        
        # Convert nodes
        for node_id, node in elements["nodes"].items():
            if node.tags:  # Only convert nodes with tags
                features.append(self._node_to_feature(node))
        
        # Convert ways
        for way_id, way in elements["ways"].items():
            if self._is_area(way):
                features.append(self._way_to_polygon(way, elements["nodes"]))
            else:
                features.append(self._way_to_linestring(way, elements["nodes"]))
        
        # Convert relations
        for rel_id, relation in elements["relations"].items():
            feature = self._relation_to_feature(relation, elements)
            if feature:
                features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    def _convert_streaming(self, elements: Dict, callback: Callable) -> None:
        """Stream conversion results through callback function."""
        for node_id, node in elements["nodes"].items():
            if node.tags:
                callback(self._node_to_feature(node))
                
        for way_id, way in elements["ways"].items():
            if self._is_area(way):
                callback(self._way_to_polygon(way, elements["nodes"]))
            else:
                callback(self._way_to_linestring(way, elements["nodes"]))
                
        for rel_id, relation in elements["relations"].items():
            feature = self._relation_to_feature(relation, elements)
            if feature:
                callback(feature)
    
    def _node_to_feature(self, node: OsmElement) -> Dict:
        """Convert an OSM node to a GeoJSON Point feature."""
        return {
            "type": "Feature",
            "id": f"node/{node.id}",
            "properties": self._make_properties(node),
            "geometry": {
                "type": "Point",
                "coordinates": [node.lon, node.lat]
            }
        }
    
    def _way_to_linestring(self, way: OsmElement, nodes: Dict) -> Dict:
        """Convert an OSM way to a GeoJSON LineString feature."""
        coordinates = [
            [nodes[node_id].lon, nodes[node_id].lat]
            for node_id in way.nodes
            if node_id in nodes
        ]
        
        return {
            "type": "Feature",
            "id": f"way/{way.id}",
            "properties": self._make_properties(way),
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates
            }
        }
    
    def _way_to_polygon(self, way: OsmElement, nodes: Dict) -> Dict:
        """Convert an OSM way representing an area to a GeoJSON Polygon feature."""
        coordinates = [
            [nodes[node_id].lon, nodes[node_id].lat]
            for node_id in way.nodes
            if node_id in nodes
        ]
        
        # Close the ring if necessary
        if coordinates[0] != coordinates[-1]:
            coordinates.append(coordinates[0])
            
        return {
            "type": "Feature",
            "id": f"way/{way.id}",
            "properties": self._make_properties(way),
            "geometry": {
                "type": "Polygon",
                "coordinates": [coordinates]
            }
        }
    
    def _relation_to_feature(self, relation: OsmElement, elements: Dict) -> Optional[Dict]:
        """Convert an OSM relation to a GeoJSON feature (if applicable)."""
        if "type" not in relation.tags:
            return None
            
        rel_type = relation.tags["type"]
        
        if rel_type == "multipolygon":
            return self._relation_to_multipolygon(relation, elements)
        
        return None
    
    def _relation_to_multipolygon(self, relation: OsmElement, elements: Dict) -> Dict:
        """Convert a multipolygon relation to a GeoJSON MultiPolygon feature."""
        outer_rings = []
        inner_rings = []
        
        for member in relation.members:
            if member["type"] != "way":
                continue
                
            way_id = member["ref"]
            if way_id not in elements["ways"]:
                continue
                
            way = elements["ways"][way_id]
            coords = [
                [elements["nodes"][node_id].lon, elements["nodes"][node_id].lat]
                for node_id in way.nodes
                if node_id in elements["nodes"]
            ]
            
            if member["role"] == "outer":
                outer_rings.append(coords)
            elif member["role"] == "inner":
                inner_rings.append(coords)
        
        # Organize rings into proper polygon structure
        polygons = []
        for outer in outer_rings:
            # Close the ring if necessary
            if outer[0] != outer[-1]:
                outer.append(outer[0])
            
            # Find inner rings that belong to this outer ring
            matching_inners = [
                inner for inner in inner_rings
                if self._point_in_polygon(inner[0], outer)
            ]
            
            # Close inner rings if necessary
            for inner in matching_inners:
                if inner[0] != inner[-1]:
                    inner.append(inner[0])
            
            polygons.append([outer] + matching_inners)
        
        return {
            "type": "Feature",
            "id": f"relation/{relation.id}",
            "properties": self._make_properties(relation),
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": polygons
            }
        }
    
    def _is_area(self, way: OsmElement) -> bool:
        """Determine if a way represents an area."""
        if way.nodes[0] != way.nodes[-1]:
            return False
            
        area_tags = {
            "area": "yes",
            "building": True,
            "landuse": True,
            "leisure": True,
            "natural": True,
            "amenity": True
        }
        
        return any(
            tag in way.tags and (
                area_tags.get(tag) is True or
                way.tags[tag] == area_tags.get(tag)
            )
            for tag in area_tags
        )
    
    def _make_properties(self, element: OsmElement) -> Dict:
        """Create GeoJSON properties from OSM element."""
        props = {
            "id": element.id,
            "type": element.type
        }
        
        if self.enhanced_properties:
            props.update(element.tags)
        else:
            for key, value in element.tags.items():
                props[f"tag_{key}"] = value
                
        return props
    
    def _point_in_polygon(self, point: List[float], polygon: List[List[float]]) -> bool:
        """Check if a point lies within a polygon using ray casting algorithm."""
        x, y = point
        inside = False
        
        for i in range(len(polygon) - 1):
            x1, y1 = polygon[i]
            x2, y2 = polygon[i + 1]
            
            if ((y1 > y) != (y2 > y) and
                x < (x2 - x1) * (y - y1) / (y2 - y1) + x1):
                inside = not inside
                
        return inside

def convert_osm_to_geojson(
    input_data: Union[str, Dict],
    enhanced_properties: bool = False,
    verbose: bool = False,
    callback: Optional[Callable] = None
) -> Dict:
    """
    Convenience function to convert OSM data to GeoJSON.
    
    Args:
        input_data: OSM data as XML string or JSON dict
        enhanced_properties: Include more structured properties if True
        verbose: Output diagnostic information if True
        callback: Optional callback function for streaming output
        
    Returns:
        Dict containing GeoJSON FeatureCollection
    """
    converter = OsmToGeoJson(
        enhanced_properties=enhanced_properties,
        verbose=verbose
    )
    return converter.convert(input_data, callback)

# Example usage:
if __name__ == "__main__":
    # Read from XML file
    with open("input.osm", "r") as f:
        osm_xml = f.read()
    
    # Convert to GeoJSON
    geojson = convert_osm_to_geojson(
        osm_xml,
        enhanced_properties=True,
        verbose=True
    )
    
    # Write to file
    with open("output.geojson", "w") as f:
        json.dump(geojson, f, indent=2)