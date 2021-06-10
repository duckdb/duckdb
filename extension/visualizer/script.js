const green = '#80ff80';
const lightGreen = '#d9ffb3';
const darkGreen = '#00cc44'
const yellow = '#ffff4d';
const red = '#ffa899';
const darkRed = '#ff0000';
const epsilon = 0.000000000000000001;
var isDiff = false;

function diff(obj1, obj2) {
    for (key in obj1) {
        if (!obj2.hasOwnProperty(key)) throw "two json files do not belong to the same query";
        {
            if (typeof obj2[key] === 'object') {
                diff(obj1[key], obj2[key]);
            } else if (obj2[key] !== obj1[key]) {
                if(obj1[key] === 0  || obj2[key] === 0){
                    obj1[key] = 1
                } else {
                    obj1[key] = (((obj1[key] / obj2[key]) * 100) / 100).toFixed(2);
                }
            }
        }

    }
}

if (secondData !== null) {
    diff(data, secondData);
    isDiff = true;
}

//configuration
const uncountedScale = d3.scaleLinear()
    .domain([0, 20, 100])
    .range([green, yellow, red]);
const diagonal = d3.linkVertical()
    .x(d => d.x + rectNode.width / 2)
    .y(d => d.y + rectNode.height)
const createPath = d3.linkVertical()
    .target(function (d) {
        const tmp = Object.assign({}, d.target);
        tmp.y = tmp.y - 150
        return tmp;
    })
    .x(d => d.x + rectNode.width / 2)
    .y(function (d) {
        return (d.y + rectNode.height)
    });
// Margins
const margin = ({top: 10, right: 10, bottom: 10, left: 10})
// Node size
const rectNode = {width: 250, height: 150, textMargin: 5};
// Node dimension
const dx = 300
const dy = 300;
// Initialize tree, root
const tree = d3.tree().nodeSize([dx, dy]);
const root = d3.hierarchy(data);
root.x0 = dx / 2;
root.y0 = 0;

var maxCardinality = Number.MIN_SAFE_INTEGER;
var minCardinality = Number.MAX_SAFE_INTEGER;
var maxTime = Number.MIN_SAFE_INTEGER;
var minTime = Number.MAX_SAFE_INTEGER;
// Preprocess Data
root.descendants().forEach((d, i) => {
// Duplicate children
    d.id = i;
    d._children = d.children;

// Uncomment to collapse nodes at start
// if (d.depth) d.children = null;

// Compute uncounted. currently not possible
    if (d.data.uncounted == null)
        d.data.uncounted = 0

// Find min and max cardinality
    if (d.data.cardinality < minCardinality) minCardinality = d.data.cardinality;
    if (d.data.cardinality > maxCardinality) maxCardinality = d.data.cardinality;
// Find min and max time
    if (i > 1) {
        if (d.data.timing < minTime) minTime = d.data.timing;
        if (d.data.timing > maxTime) maxTime = d.data.timing;
    }
});

//Node color
var rectColor;
if (!isDiff) {
    rectColor = d3.scaleLinear()
        .domain([Number.MIN_SAFE_INTEGER, minTime, (maxTime + minTime) / 2, (maxTime + minTime) / 2 + epsilon, maxTime, Number.MAX_SAFE_INTEGER])
        .range([darkGreen, green, lightGreen, yellow, red, darkRed]);

} else {
    rectColor = d3.scaleLinear()
        .domain([Number.MIN_SAFE_INTEGER, minTime, 1, 1 + epsilon,  maxTime, Number.MAX_SAFE_INTEGER])
        .range([darkRed, red, yellow, lightGreen, green, darkGreen]);

}

const widthScale = d3.scaleLinear()
    .domain([minCardinality, maxCardinality])
    .range([5, 75]);

const bodyMain = d3.select("body")
    .style("width", "100%")
    .style("height", "100%");
// Width and Height
const height = bodyMain.node().getBoundingClientRect().height;
const width = bodyMain.node().getBoundingClientRect().width;

const svgMain = bodyMain.append('div')
    .style("width", width)
    .style("height", height)
    .append("svg")
    .style("width", width)
    .style("height", height);

const gTree = svgMain.append("g")

const gLink = gTree.append("g")
    .attr("fill", "none")
    .attr("stroke", "#555")
    .attr("stroke-opacity", 0.4)
    .attr("stroke-width", 1.5);

const gNode = gTree.append("g")

const zoomMain = d3.zoom()
    .on('zoom', (event) => gTree.attr('transform', event.transform))
    .scaleExtent([0.05, 3])


svgMain.call(zoomMain);
zoomMain.translateTo(svgMain, -width / 2, 0);
zoomToFit();

// Update tree
function update(e, source) {
    const duration = 500;
    const nodes = root.descendants().reverse();
    const links = root.links();

// Compute the new tree layout.
    tree(root);

    const transition = svgMain.transition()
        .duration(duration)

// Update the nodes…
    const node = gNode.selectAll("g")
        .data(nodes, d => d.id);

// Enter any new modes at the parent's previous position.
    const nodeEnter = node.enter().append('g')
        .attr('class', 'node')
        .attr("transform", function (d) {
            return "translate(" + source.x0 + "," + source.y0 + ")";
        });

// Add rect for the nodes
    const rect = nodeEnter.append('rect')
        .attr('rx', 20)
        .attr('ry', 20)
        .attr('width', rectNode.width)
        .attr('height', rectNode.height)
        .attr('class', 'node-rect')
        .style('fill', function (d) {
            return rectColor(d.data.timing)
        })
        .attr('fill-opacity', '0.2')
        .attr('stroke-width', '3')
        .attr('stroke-opacity', '0.2')
        .attr('stroke', "black")

    nodeEnter.append('foreignObject')
        .attr('x', rectNode.textMargin)
        .attr('y', rectNode.textMargin)
        .attr('width', rectNode.width)
        .attr('height', rectNode.height)
        .style("font-size", "16px")
        .style("font-family", "Verdana")
        .append('xhtml').html(function (d) {
        return '<div style="width: '
            + (rectNode.width - rectNode.textMargin * 2) + 'px; height: '
            + (rectNode.height - rectNode.textMargin * 2) + 'px;" class="node-text wordwrap">'
            + '<center><b>' + d.data.name + '</b><br><br></center>'
            + '<center><b>Cardinality: </b>' + d.data.cardinality + '<br></center>'
            + '<center><b>Uncounted: </b>' + d.data.uncounted + '<br></center>'
            + '<center><b>Timing: </b>' + d.data.timing + '<br><center>'
            + '</div>';
    })
        .on('click', (e, d) => display_table(e, d));

// Add backgroung circle
    nodeEnter.append("circle")
        .attr("r", 20)
        .attr("fill", "white")
        .attr("transform", function (d) {
            return "translate(" + rectNode.width / 2 + ", " + rectNode.height + ")";
        });

// Add toggle-button for the nodes
    nodeEnter.append("circle")
        .classed("button", true)
        .attr("r", 20)
        .attr("fill", "blue")
        .attr("stroke-width", 10)
        .on("click", (e, d) => {
            d.children = d.children ? null : d._children;
            update(e, d);
// zoomToFit();
        })

    nodeEnter.selectAll(".button")
        .attr("transform", function (d) {
            return "translate(" + rectNode.width / 2 + ", " + rectNode.height + ")";
        })
        .attr("fill-opacity", d => d._children ? 0.5 : 0.2);


// Transition nodes to their new position.
    const nodeUpdate = node.merge(nodeEnter).transition(transition)
        .attr("transform", d => `translate(${d.x},${d.y})`)
        .attr("fill-opacity", 1)
        .attr("stroke-opacity", 1);

// Transition exiting nodes to the parent's new position.
    const nodeExit = node.exit().transition(transition).remove()
        .attr("transform", d => `translate(${source.x},${source.y})`)
        .attr("fill-opacity", 0)
        .attr("stroke-opacity", 0);

// Update the links…
    const link = gLink.selectAll("path")
        .data(links, d => d.target.id);

// Enter any new links at the parent's previous position.
    const linkEnter = link.enter().append("path")
        .attr("class", "link")
        .attr("d", d => {
            const o = {x: source.x0, y: source.y0};
            return diagonal({source: o, target: o});
        })
        .style('stroke-width', function (d) {
            return widthScale(d.target.data.cardinality)
        })

// Transition links to their new position.
    link.merge(linkEnter).transition(transition)
        .style('stroke-width', function (d) {
            return widthScale(d.target.data.cardinality)
        })
        .attr('d', function (d) {
            return createPath(d)
        })


// Transition exiting nodes to the parent's new position.
    link.exit().transition(transition).remove()
        .attr("d", d => {
            const o = {x: source.x, y: source.y};
            return diagonal({source: o, target: o});
        });

// Stash the old positions for transition.
    root.eachBefore(d => {
        d.x0 = d.x;
        d.y0 = d.y;
    });
}

// Zoom
function zoomToFit(paddingPercent) {
    const bounds = gTree.node().getBBox();
    const parent = svgMain.node().parentElement;
    const fullWidth = parent.clientWidth;
    const fullHeight = parent.clientHeight;

    const width = bounds.width;
    const height = bounds.height;

    const midX = bounds.x + (width / 2);
    const midY = bounds.y + (height / 2);

    if (width === 0 || height === 0) return; // nothing to fit

    const scale = (paddingPercent || 0.75) / Math.max(width / fullWidth, height / fullHeight);
    const translate = [fullWidth / 2 - scale * midX, fullHeight / 2 - scale * midY];

    const transform = d3.zoomIdentity
        .translate(translate[0], translate[1])
        .scale(scale);

    svgMain
        .transition()
        .duration(500)
        .call(zoomMain.transform, transform);
}


// Display the table
function display_table(e, d) {
// Blur the background
    gLink.style("opacity", 0.4)
    gNode.style("opacity", 0.4)

    const div = bodyMain.append('div')
        .style("z-index", "2")
        .style("width", width)
        .style("height", height)
        .on("click", function (d) {
            div.remove();
// UnBlur the background
            gLink.style("opacity", 1);
            gNode.style("opacity", 1);
        });

    const svg = div
        .append("svg")
        .style("width", width)
        .style("height", height);

    const gTable = svg.append('g')
        .attr("transform", `translate(0,0)`);

    const zoom = d3.zoom()
        .on('zoom', (event) => gTable.attr('transform', event.transform))
        .scaleExtent([0.05, 3])
    svg.call(zoom)

// Load rows and headers
    const rows = []
    var headers = []
// Check to see if time is provided or cycle_per_tuple
    var index;
    var measure_unit;
    d.data.timings.forEach(d => {
        rows.push(Object.values(d));
    })
    if (!(d.data.timings.length === 0)) {
        headers = Object.keys(d.data.timings[0])
        headers.forEach((d, i) => {
            if (d === "timing") index = i
        });
        (rows[0][index] === "NULL") ? measure_unit = "cycles_per_tuple" : "timing";
    }


    headers.forEach((d, i) => {
        if (d === measure_unit) index = i
    });
    var min = Number.MAX_SAFE_INTEGER;
    var max = Number.MIN_SAFE_INTEGER;
    rows.forEach((v, i) => {
        if (v[index] < min) min = v[index];
        if (v[index] > max) max = v[index];
    })

    if (!isDiff) {
        var rowColorScale = d3.scaleLinear()
            .domain([Number.MIN_SAFE_INTEGER, min, (min + max) / 2, (min + max) / 2 + epsilon, max, Number.MAX_SAFE_INTEGER])
            .range([darkGreen, green, lightGreen, yellow, red, darkRed]);
    } else {
        rowColorScale = d3.scaleLinear()
            .domain([Number.MIN_SAFE_INTEGER, min, 1, 1 + epsilon, max, Number.MAX_SAFE_INTEGER])
            .range([darkRed, red, yellow, lightGreen, green, darkGreen]);
    }


    const foreignObject = gTable.append("foreignObject")
        .attr("width", 1000)
        .attr("height", 1000)

// Table
    const table = foreignObject.append('xhtml:table')
        .style("border-collapse", "collapse")
        .style("border", "2px black solid")
        .style("margin", "auto");

// headers
    table.append("thead").append("tr")
        .selectAll("th")
        .data(headers)
        .enter().append("th")
        .text(function (d) {
            return d;
        })
        .style("border", "1px black solid")
        .style("padding", "5px")
        .style("background-color", "lightgray")
        .style("font-size", "16px")
        .style("font-family", "Verdana")
        .style("font-weight", "bold")
        .style("text-transform", "uppercase");

// data
    table.append("tbody")
        .selectAll("tr").data(rows)
        .enter().append("tr")
        .style("font-size", "12px")
        .style("font-family", "Verdana")
        .style("background-color", d => {
            return rowColorScale(d[index]);
        })
        .selectAll("td")
        .data(function (d) {
            return d;
        })
        .enter().append("td")
        .style("text-align", "center")
        .style("border", "1px black solid")
        .style("padding", "10px")
        .on("mouseover", function () {
            d3.select(this).style("font-size", "20px")
        })
        .on("mouseout", function () {
            d3.select(this).style("font-size", "12px")
        })
        .text(function (d) {
            return d;
        })
}

// Main()
update(event, root);