import json
from typing import Dict, List, Union, Optional
import xml.etree.ElementTree as ET
from dataclasses import dataclass

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
    def __init__(self):
        """Initialize the OSM to GeoJSON converter."""
        pass
        
    def convert(self, input_data: Union[str, Dict]) -> Dict:
        """
        Convert OSM data to GeoJSON.
        
        Args:
            input_data: Either XML string or JSON dict of OSM data
            
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
            
        elements = self._parse_elements(data)
        return self._convert_to_geojson(elements)
    
    def _parse_xml(self, xml_str: str) -> Dict:
        """Parse OSM XML format into internal dictionary format."""
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
    
    def _convert_to_geojson(self, elements: Dict) -> Dict:
        """Convert parsed OSM elements to GeoJSON FeatureCollection."""
        features = []
        processed_ways = set()
        processed_nodes = set()
        
        # Process relations first
        for relation in elements["relations"].values():
            if "type" in relation.tags and relation.tags["type"] in ["route", "superroute"]:
                route_feature = self._convert_route(relation, elements)
                if route_feature:
                    features.append(route_feature)
                    # Mark ways as processed
                    for member in relation.members:
                        if member["type"] == "way":
                            processed_ways.add(member["ref"])
        
        # Process remaining ways
        for way_id, way in elements["ways"].items():
            if way_id not in processed_ways:
                way_feature = self._convert_way(way, elements["nodes"])
                if way_feature:
                    features.append(way_feature)
                    # Mark nodes as processed
                    processed_nodes.update(way.nodes)
        
        # Process remaining nodes with tags
        for node_id, node in elements["nodes"].items():
            if node_id not in processed_nodes and node.tags:
                features.append({
                    "type": "Feature",
                    "id": f"node/{node.id}",
                    "properties": {"id": node.id, "type": "node", **node.tags},
                    "geometry": {
                        "type": "Point",
                        "coordinates": [node.lon, node.lat]
                    }
                })
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    def _convert_way(self, way: OsmElement, nodes: Dict) -> Optional[Dict]:
        """Convert a way to a GeoJSON feature."""
        coordinates = [
            [nodes[node_id].lon, nodes[node_id].lat]
            for node_id in way.nodes
            if node_id in nodes
        ]
        
        if not coordinates:
            return None
            
        return {
            "type": "Feature",
            "id": f"way/{way.id}",
            "properties": {"id": way.id, "type": "way", **way.tags},
            "geometry": {
                "type": "LineString",
                "coordinates": coordinates
            }
        }
    
    def _convert_route(self, relation: OsmElement, elements: Dict) -> Optional[Dict]:
        """Convert a route relation to a GeoJSON feature."""
        route_ways = []
        current_line = []
        
        for member in relation.members:
            if member["type"] != "way" or member["ref"] not in elements["ways"]:
                continue
                
            way = elements["ways"][member["ref"]]
            coords = [
                [elements["nodes"][node_id].lon, elements["nodes"][node_id].lat]
                for node_id in way.nodes
                if node_id in elements["nodes"]
            ]
            
            if not coords:
                continue
                
            if not current_line:
                current_line.extend(coords)
            else:
                if coords[0] == current_line[-1]:
                    current_line.extend(coords[1:])
                elif coords[-1] == current_line[-1]:
                    current_line.extend(reversed(coords[:-1]))
                else:
                    if len(current_line) > 1:
                        route_ways.append(current_line)
                    current_line = coords
        
        if len(current_line) > 1:
            route_ways.append(current_line)
            
        if not route_ways:
            return None
            
        return {
            "type": "Feature",
            "id": f"relation/{relation.id}",
            "properties": {
                "id": relation.id,
                "type": "relation",
                "route_type": relation.tags.get("route", "unknown"),
                "name": relation.tags.get("name", ""),
                **relation.tags
            },
            "geometry": {
                "type": "MultiLineString",
                "coordinates": route_ways
            }
        }

def convert_osm_to_geojson(input_data: Union[str, Dict]) -> Dict:
    """
    Convert OSM data to GeoJSON.
    
    Args:
        input_data: OSM data as XML string or JSON dict
        
    Returns:
        Dict containing GeoJSON FeatureCollection
    """
    converter = OsmToGeoJson()
    return converter.convert(input_data)

if __name__ == "__main__":
    # Example usage
    with open("input.osm", "r") as f:
        osm_xml = f.read()
    
    geojson = convert_osm_to_geojson(osm_xml)
    
    with open("output.geojson", "w") as f:
        json.dump(geojson, f, indent=2)