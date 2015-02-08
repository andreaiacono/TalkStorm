var width = 800, height = 400;

var projection = d3.geo.equirectangular()
                    .scale(130)
                    .translate([width/2 , height / 2])
                    .precision(0);
var path = d3.geo.path().projection(projection);

var svg = d3.select("#map").append("svg")
            .attr("width", width)
            .attr("height", height);

d3.json("/static/world.json", function(error, data) {
    svg.selectAll(".bgback")
    .data(topojson.feature(data, data.objects.land).features)
    .enter()
    .append("path")
    .attr("class", "bgback")
    .attr("d", path)
    .style("stroke", "#000")
    .style("stroke-width", 0.8);

});
