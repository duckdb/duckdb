/*
* Treant.js
*
* (c) 2013 Fran Peručić
*
* Treant is an open-source JavaScipt library for visualization of tree diagrams.
* It implements the node positioning algorithm of John Q. Walker II "Positioning nodes for General Trees".
*
* References:
* Emilio Cortegoso Lobato: ECOTree.js v1.0 (October 26th, 2006)
*
*/

;(function(){

	var UTIL = {
		inheritAttrs: function(me, from) {
			for (var attr in from) {
				if(typeof from[attr] !== 'function') {
					if(me[attr] instanceof Object && from[attr] instanceof Object) {
						this.inheritAttrs(me[attr], from[attr]);
					} else {
						me[attr] = from[attr];
					}
				}
			}
		},

		createMerge: function(obj1, obj2) {
			var newObj = {};
			if(obj1) this.inheritAttrs(newObj, this.cloneObj(obj1));
			if(obj2) this.inheritAttrs(newObj, obj2);
			return newObj;
		},

		cloneObj: function (obj) {
			if (Object(obj) !== obj) {
				return obj;
			}
			var res = new obj.constructor();
			for (var key in obj) if (obj["hasOwnProperty"](key)) {
				res[key] = this.cloneObj(obj[key]);
			}
			return res;
		},
		addEvent: function(el, eventType, handler) {
			if (el.addEventListener) { // DOM Level 2 browsers
				el.addEventListener(eventType, handler, false);
			} else if (el.attachEvent) { // IE <= 8
				el.attachEvent('on' + eventType, handler);
			} else { // ancient browsers
				el['on' + eventType] = handler;
			}
		},

		hasClass: function(element, my_class) {
			return (" " + element.className + " ").replace(/[\n\t]/g, " ").indexOf(" "+my_class+" ") > -1;
		}
	};

	/**
	* ImageLoader constructor.
	* @constructor
	* ImageLoader is used for determening if all the images from the Tree are loaded.
	* 	Node size (width, height) can be correcty determined only when all inner images are loaded
	*/
	var ImageLoader = function() {
		this.loading = [];
	};


	ImageLoader.prototype = {
		processNode: function(node) {
			var images = node.nodeDOM.getElementsByTagName('img'),
				i =	images.length;
			while(i--) {
				this.create(node, images[i]);
			}
		},

		removeAll: function(img_src) {
			var i = this.loading.length;
			while (i--) {
				if (this.loading[i] === img_src) { this.loading.splice(i,1); }
			}
		},

		create: function (node, image) {

			var self = this,
				source = image.src;
			this.loading.push(source);

			function imgTrigger() {
				self.removeAll(source);
				node.width = node.nodeDOM.offsetWidth;
				node.height = node.nodeDOM.offsetHeight;
			}

			if (image.complete) { return imgTrigger(); }

			UTIL.addEvent(image, 'load', imgTrigger);
			UTIL.addEvent(image, 'error', imgTrigger); // handle broken url-s

			// load event is not fired for cached images, force the load event
			image.src += "?" + new Date().getTime();
		},
		isNotLoading: function() {
			return this.loading.length === 0;
		}
	};

	/**
	* Class: TreeStore
	* @singleton
	* TreeStore is used for holding initialized Tree objects
	* 	Its purpose is to avoid global variables and enable multiple Trees on the page.
	*/

	var TreeStore = {
		store: [],
		createTree: function(jsonConfig) {
			this.store.push(new Tree(jsonConfig, this.store.length));
			return this.store[this.store.length - 1]; // return newly created tree
		},
		get: function (treeId) {
			return this.store[ treeId ];
		}
	};

	/**
	* Tree constructor.
	* @constructor
	*/
	var Tree = function (jsonConfig, treeId) {

		this.id = treeId;

		this.imageLoader = new ImageLoader();
		this.CONFIG = UTIL.createMerge(Tree.prototype.CONFIG, jsonConfig['chart']);
		this.drawArea = document.getElementById(this.CONFIG.container.substring(1));
		this.drawArea.className += " Treant";
		this.nodeDB = new NodeDB(jsonConfig['nodeStructure'], this);

		// key store for storing reference to node connectors,
		// key = nodeId where the connector ends
		this.connectionStore = {};
	};

	Tree.prototype = {

		positionTree: function(callback) {

			var self = this;

			if (this.imageLoader.isNotLoading()) {

				var root = this.root(),
					orient = this.CONFIG.rootOrientation;

				this.resetLevelData();
				
				this.firstWalk(root, 0);
				this.secondWalk( root, 0, 0, 0 );
				
				this.positionNodes();

				if (this.CONFIG['animateOnInit']) {
					setTimeout(function() { root.toggleCollapse(); }, this.CONFIG['animateOnInitDelay']);
				}

				if(!this.loaded) {
					this.drawArea.className += " loaded"; // nodes are hidden until .loaded class is add
					if (Object.prototype.toString.call(callback) === "[object Function]") { callback(self); }
					this.loaded = true;
				}

			} else {
				setTimeout(function() { self.positionTree(callback); }, 10);
			}
		},

		/*
		* In a first post-order walk, every node of the tree is
		* assigned a preliminary x-coordinate (held in field
		* node->flPrelim). In addition, internal nodes are
		* given modifiers, which will be used to move their
		* children to the right (held in field
		* node->flModifier).
		*/
		firstWalk: function(node, level) {

			node.prelim = null; node.modifier = null;

			this.setNeighbors(node, level);
			this.calcLevelDim(node, level);

			var leftSibling = node.leftSibling();

			if(node.childrenCount() === 0 || level == this.CONFIG.maxDepth) {
				// set preliminary x-coordinate
				if(leftSibling) {
					node.prelim = leftSibling.prelim + leftSibling.size() + this.CONFIG.siblingSeparation;
				} else {
					node.prelim = 0;
				}

			} else {
				//node is not a leaf,  firstWalk for each child
				for(var i = 0, n = node.childrenCount(); i < n; i++) {
					this.firstWalk(node.childAt(i), level + 1);
				}

				var midPoint = node.childrenCenter() - node.size() / 2;

				if(leftSibling) {
					node.prelim		= leftSibling.prelim + leftSibling.size() + this.CONFIG.siblingSeparation;
					node.modifier	= node.prelim - midPoint;
					this.apportion( node, level );
				} else {
					node.prelim = midPoint;
				}

				// handle stacked children positioning
				if(node.stackParent) { // hadle the parent of stacked children
					node.modifier += this.nodeDB.get( node.stackChildren[0] ).size()/2 + node.connStyle.stackIndent;
				} else if ( node.stackParentId ) { // handle stacked children
					node.prelim = 0;
				}
			}
		},

		/*
		* Clean up the positioning of small sibling subtrees.
		* Subtrees of a node are formed independently and
		* placed as close together as possible. By requiring
		* that the subtrees be rigid at the time they are put
		* together, we avoid the undesirable effects that can
		* accrue from positioning nodes rather than subtrees.
		*/
		apportion: function (node, level) {
			var firstChild				= node.firstChild(),
				firstChildLeftNeighbor	= firstChild.leftNeighbor(),
				compareDepth			= 1,
				depthToStop				= this.CONFIG.maxDepth - level;

			while( firstChild && firstChildLeftNeighbor && compareDepth <= depthToStop ) {
				// calculate the position of the firstChild, according to the position of firstChildLeftNeighbor

				var modifierSumRight	= 0,
					modifierSumLeft		= 0,
					leftAncestor		= firstChildLeftNeighbor,
					rightAncestor		= firstChild;

				for(var i = 0; i < compareDepth; i++) {

					leftAncestor		= leftAncestor.parent();
					rightAncestor		= rightAncestor.parent();
					modifierSumLeft		+= leftAncestor.modifier;
					modifierSumRight	+= rightAncestor.modifier;
					// all the stacked children are oriented towards right so use right variables
					if(rightAncestor.stackParent !== undefined) modifierSumRight += rightAncestor.size()/2;
				}

				// find the gap between two trees and apply it to subTrees
				// and mathing smaller gaps to smaller subtrees

				var totalGap = (firstChildLeftNeighbor.prelim + modifierSumLeft + firstChildLeftNeighbor.size() + this.CONFIG.subTeeSeparation) - (firstChild.prelim + modifierSumRight );

				if(totalGap > 0) {

					var subtreeAux = node,
						numSubtrees = 0;

					// count all the subtrees in the LeftSibling
					while(subtreeAux && subtreeAux.id != leftAncestor.id) {
						subtreeAux = subtreeAux.leftSibling();
						numSubtrees++;
					}

					if(subtreeAux) {

						var subtreeMoveAux = node,
							singleGap = totalGap / numSubtrees;

						while(subtreeMoveAux.id != leftAncestor.id) {
							subtreeMoveAux.prelim	+= totalGap;
							subtreeMoveAux.modifier	+= totalGap;
							totalGap				-= singleGap;
							subtreeMoveAux = subtreeMoveAux.leftSibling();
						}
					}
				}

				compareDepth++;

				if(firstChild.childrenCount() === 0){
					firstChild = node.leftMost(0, compareDepth);
				} else {
					firstChild = firstChild.firstChild();
				}
				if(firstChild) {
					firstChildLeftNeighbor = firstChild.leftNeighbor();
				}
			}
		},

		/*
		* During a second pre-order walk, each node is given a
	    * final x-coordinate by summing its preliminary
	    * x-coordinate and the modifiers of all the node's
	    * ancestors.  The y-coordinate depends on the height of
	    * the tree.  (The roles of x and y are reversed for
	    * RootOrientations of EAST or WEST.)
		*/
		secondWalk: function( node, level, X, Y) {

			if(level <= this.CONFIG.maxDepth) {
				var xTmp = node.prelim + X,
					yTmp = Y, align = this.CONFIG.nodeAlign,
					orinet = this.CONFIG.rootOrientation,
					levelHeight, nodesizeTmp;

				if (orinet == 'NORTH' || orinet == 'SOUTH') {

					levelHeight = this.levelMaxDim[level].height;
					nodesizeTmp = node.height;
					if (node.pseudo) node.height = levelHeight; // assign a new size to pseudo nodes
				}
				else if (orinet == 'WEST' || orinet == 'EAST') {

					levelHeight = this.levelMaxDim[level].width;
					nodesizeTmp = node.width;
					if (node.pseudo) node.width = levelHeight; // assign a new size to pseudo nodes
				}

				node.X = xTmp;

				if (node.pseudo) { // pseudo nodes need to be properly aligned, otherwise position is not correct in some examples
					if (orinet == 'NORTH' || orinet == 'WEST') {
						node.Y = yTmp; // align "BOTTOM"
					}
					else if (orinet == 'SOUTH' || orinet == 'EAST') {
						node.Y = (yTmp + (levelHeight - nodesizeTmp)); // align "TOP"
					}
				
				} else {
					node.Y = ( align == 'CENTER' ) ? (yTmp + (levelHeight - nodesizeTmp) / 2) :
							( align == 'TOP' )	? (yTmp + (levelHeight - nodesizeTmp)) :
							yTmp;
				}


				if(orinet == 'WEST' || orinet == 'EAST') {
					var swapTmp = node.X;
					node.X = node.Y;
					node.Y = swapTmp;
				}

				if (orinet == 'SOUTH') {

					node.Y = -node.Y - nodesizeTmp;
				}
				else if (orinet == 'EAST') {

					node.X = -node.X - nodesizeTmp;
				}

				if(node.childrenCount() !== 0) {

					if(node.id === 0 && this.CONFIG.hideRootNode) {
						// ako je root node Hiden onda nemoj njegovu dijecu pomaknut po Y osi za Level separation, neka ona budu na vrhu
						this.secondWalk(node.firstChild(), level + 1, X + node.modifier, Y);
					} else {

						this.secondWalk(node.firstChild(), level + 1, X + node.modifier, Y + levelHeight + this.CONFIG.levelSeparation);
					}
				}

				if(node.rightSibling()) {

					this.secondWalk(node.rightSibling(), level, X, Y);
				}
			}
		},

		// position all the nodes, center the tree in center of its container
		// 0,0 coordinate is in the upper left corner
		positionNodes: function() {

			var self = this,
				treeSize = {
					x: self.nodeDB.getMinMaxCoord('X', null, null),
					y: self.nodeDB.getMinMaxCoord('Y', null, null)
				},

				treeWidth = treeSize.x.max - treeSize.x.min,
				treeHeight = treeSize.y.max - treeSize.y.min,

				treeCenter = {
					x: treeSize.x.max - treeWidth/2,
					y: treeSize.y.max - treeHeight/2
				},

				containerCenter = {
					x: self.drawArea.clientWidth/2,
					y: self.drawArea.clientHeight/2
				},

				deltaX = containerCenter.x - treeCenter.x,
				deltaY = containerCenter.y - treeCenter.y,

				// all nodes must have positive X or Y coordinates, handle this with offsets
				negOffsetX = ((treeSize.x.min + deltaX) <= 0) ? Math.abs(treeSize.x.min) : 0,
				negOffsetY = ((treeSize.y.min + deltaY) <= 0) ? Math.abs(treeSize.y.min) : 0,
				i, len, node;

			this.handleOverflow(treeWidth, treeHeight);

			// position all the nodes
			for(i =0, len = this.nodeDB.db.length; i < len; i++) {

				node = this.nodeDB.get(i);

				if(node.id === 0 && this.CONFIG.hideRootNode) continue;

				// if the tree is smaller than the draw area, then center the tree within drawing area
				node.X += negOffsetX + ((treeWidth < this.drawArea.clientWidth) ? deltaX : this.CONFIG.padding);
				node.Y += negOffsetY + ((treeHeight < this.drawArea.clientHeight) ? deltaY : this.CONFIG.padding);

				var collapsedParent = node.collapsedParent(),
					hidePoint = null;

				if(collapsedParent) {
					// position the node behind the connector point of the parent, so future animations can be visible
					hidePoint = collapsedParent.connectorPoint( true );
					node.hide(hidePoint);

				} else if(node.positioned) {
					// node is allready positioned, 
					node.show();
				} else { // inicijalno stvaranje nodeova, postavi lokaciju
					node.nodeDOM.style.left = node.X + 'px';
					node.nodeDOM.style.top = node.Y + 'px';

					node.positioned = true;
				}
				
				if (node.id !== 0 && !(node.parent().id === 0 && this.CONFIG.hideRootNode)) {
					this.setConnectionToParent(node, hidePoint); // skip the root node
				} 
				else if (!this.CONFIG.hideRootNode && node.drawLineThrough) {
					// drawlinethrough is performed for for the root node also
					node.drawLineThroughMe();
				}
			}

		},

		// create Raphael instance, set scrollbars if necessary
		handleOverflow: function(treeWidth, treeHeight) {

			var viewWidth = (treeWidth < this.drawArea.clientWidth) ? this.drawArea.clientWidth : treeWidth + this.CONFIG.padding*2,
				viewHeight = (treeHeight < this.drawArea.clientHeight) ? this.drawArea.clientHeight : treeHeight + this.CONFIG.padding*2;

			if(this._R) {
				this._R.setSize(viewWidth, viewHeight);
			} else {
				this._R = this._R || Raphael(this.drawArea, viewWidth, viewHeight);
			}


			if(this.CONFIG.scrollbar == 'native') {

				if(this.drawArea.clientWidth < treeWidth) { // is owerflow-x necessary
					this.drawArea.style.overflowX = "auto";
				}

				if(this.drawArea.clientHeight < treeHeight) { // is owerflow-y necessary
					this.drawArea.style.overflowY = "auto";
				}

			} else if (this.CONFIG.scrollbar == 'fancy') {

				var jq_drawArea = $(this.drawArea);
				if (jq_drawArea.hasClass('ps-container')) { // znaci da je 'fancy' vec inicijaliziran, treba updateat

					jq_drawArea.find('.Treant').css({
						width: viewWidth,
						height: viewHeight
					});

					jq_drawArea.perfectScrollbar('update');

				} else {

					var mainContiner = jq_drawArea.wrapInner('<div class="Treant"/>'),
						child = mainContiner.find('.Treant');

					child.css({
						width: viewWidth,
						height: viewHeight
					});

					mainContiner.perfectScrollbar();
				}
			} // else this.CONFIG.scrollbar == 'None'

		},

		setConnectionToParent: function(node, hidePoint) {

			var stacked = node.stackParentId,
				connLine,
				parent = stacked ? this.nodeDB.get(stacked) : node.parent(),

				pathString = hidePoint ? this.getPointPathString(hidePoint):
							this.getPathString(parent, node, stacked);

			if (this.connectionStore[node.id]) {
				// connector allready exists, update the connector geometry
				connLine = this.connectionStore[node.id];
				this.animatePath(connLine, pathString);

			} else {

				connLine = this._R.path( pathString );
				this.connectionStore[node.id] = connLine;

				// don't show connector arrows por pseudo nodes
				if(node.pseudo) { delete parent.connStyle.style['arrow-end']; }
				if(parent.pseudo) { delete parent.connStyle.style['arrow-start']; }

				connLine.attr(parent.connStyle.style);

				if(node.drawLineThrough || node.pseudo) { node.drawLineThroughMe(hidePoint); }
			}
		},

		// create the parh which is represanted as a point, used for hideing the connection
		getPointPathString: function(hp) {
			// "_" indicates the path will be hidden
			return ["_M", hp.x, ",", hp.y, 'L', hp.x, ",", hp.y, hp.x, ",", hp.y].join(" ");
		},

		animatePath: function(path, pathString) {

			if (path.hidden && pathString.charAt(0) !== "_") { // path will be shown, so show it
				path.show();
				path.hidden = false;
			}

			path.animate({
				path: pathString.charAt(0) === "_" ? pathString.substring(1) : pathString // remove the "_" prefix if it exists
			}, this.CONFIG['animation']['connectorsSpeed'],  this.CONFIG['animation']['connectorsAnimation'],
			function(){
				if(pathString.charAt(0) === "_") { // animation is hideing the path, hide it at the and of animation
					path.hide();
					path.hidden = true;
				}

			});
			
		},

		getPathString: function(from_node, to_node, stacked) {

			var startPoint = from_node.connectorPoint( true ),
				endPoint = to_node.connectorPoint( false ),
				orinet = this.CONFIG.rootOrientation,
				connType = from_node.connStyle.type,
				P1 = {}, P2 = {};

			if (orinet == 'NORTH' || orinet == 'SOUTH') {
				P1.y = P2.y = (startPoint.y + endPoint.y) / 2;

				P1.x = startPoint.x;
				P2.x = endPoint.x;

			} else if (orinet == 'EAST' || orinet == 'WEST') {
				P1.x = P2.x = (startPoint.x + endPoint.x) / 2;

				P1.y = startPoint.y;
				P2.y = endPoint.y;
			}

			// sp, p1, pm, p2, ep == "x,y"
			var sp = startPoint.x+','+startPoint.y, p1 = P1.x+','+P1.y, p2 = P2.x+','+P2.y, ep = endPoint.x+','+endPoint.y,
				pm = (P1.x + P2.x)/2 +','+ (P1.y + P2.y)/2, pathString, stackPoint;

			if(stacked) { // STACKED CHILDREN

				stackPoint = (orinet == 'EAST' || orinet == 'WEST') ?
								endPoint.x+','+startPoint.y :
								startPoint.x+','+endPoint.y;

				if( connType == "step" || connType == "straight" ) {

					pathString = ["M", sp, 'L', stackPoint, 'L', ep];

				} else if ( connType == "curve" || connType == "bCurve" ) {

					var helpPoint, // used for nicer curve lines
						indent = from_node.connStyle.stackIndent;

					if (orinet == 'NORTH') {
						helpPoint = (endPoint.x - indent)+','+(endPoint.y - indent);
					} else if (orinet == 'SOUTH') {
						helpPoint = (endPoint.x - indent)+','+(endPoint.y + indent);
					} else if (orinet == 'EAST') {
						helpPoint = (endPoint.x + indent) +','+startPoint.y;
					} else if ( orinet == 'WEST') {
						helpPoint = (endPoint.x - indent) +','+startPoint.y;
					}

					pathString = ["M", sp, 'L', helpPoint, 'S', stackPoint, ep];
				}

			} else {  // NORAML CHILDREN

				if( connType == "step" ) {
					pathString = ["M", sp, 'L', p1, 'L', p2, 'L', ep];
				} else if ( connType == "curve" ) {
					pathString = ["M", sp, 'C', p1, p2, ep ];
				} else if ( connType == "bCurve" ) {
					pathString = ["M", sp, 'Q', p1, pm, 'T', ep];
				} else if (connType == "straight" ) {
					pathString = ["M", sp, 'L', sp, ep];
				}
			}

			return pathString.join(" ");
		},

		// algorithm works from left to right, so previous processed node will be left neigbor of the next node
		setNeighbors: function(node, level) {

			node.leftNeighborId = this.lastNodeOnLevel[level];
			if(node.leftNeighborId) node.leftNeighbor().rightNeighborId = node.id;
			this.lastNodeOnLevel[level] = node.id;
		},

		// used for calculation of height and width of a level (level dimensions)
		calcLevelDim: function(node, level) { // root node is on level 0
			if (this.levelMaxDim[level]) {
				if( this.levelMaxDim[level].width < node.width )
					this.levelMaxDim[level].width = node.width;

				if( this.levelMaxDim[level].height < node.height )
					this.levelMaxDim[level].height = node.height;

			} else {
				this.levelMaxDim[level] = { width: node.width, height: node.height };
			}
		},

		resetLevelData: function() {
			this.lastNodeOnLevel = [];
			this.levelMaxDim = [];
		},

		root: function() {
			return this.nodeDB.get( 0 );
		}
	};

	/**
	* NodeDB constructor.
	* @constructor
	* NodeDB is used for storing the nodes. Each tree has its own NodeDB.
	*/
	var NodeDB = function (nodeStructure, tree) {

		this.db	= [];

		var self = this;

		function itterateChildren(node, parentId) {

			var newNode = self.createNode(node, parentId, tree, null);

			if(node['children']) {

				newNode.children = [];

				// pseudo node is used for descending children to the next level
				if(node['childrenDropLevel'] && node['childrenDropLevel'] > 0) {
					while(node['childrenDropLevel']--) {
						// pseudo node needs to inherit the connection style from its parent for continuous connectors
						var connStyle = UTIL.cloneObj(newNode.connStyle);
						newNode = self.createNode('pseudo', newNode.id, tree, null);
						newNode.connStyle = connStyle;
						newNode.children = [];
					}
				}

				var stack = (node['stackChildren'] && !self.hasGrandChildren(node)) ? newNode.id : null;

				// svildren are position on separate leves, one beneeth the other
				if (stack !== null) { newNode.stackChildren = []; }

				for (var i = 0, len = node['children'].length; i < len ; i++) {

					if (stack !== null) {
						newNode =  self.createNode(node['children'][i], newNode.id, tree, stack);
						if((i + 1) < len) newNode.children = []; // last node cant have children
					} else {
						itterateChildren(node['children'][i], newNode.id);
					}
				}
			}
		}

		if (tree.CONFIG['animateOnInit']) nodeStructure['collapsed'] = true;

		itterateChildren( nodeStructure, -1); // root node

		this.createGeometries(tree);
	};

	NodeDB.prototype = {

		createGeometries: function(tree) {
			var i = this.db.length, node;
			while(i--) {
				this.get(i).createGeometry(tree);
			}
		},
		
		get: function (nodeId) {
			return this.db[nodeId]; // get node by ID
		},

		createNode: function(nodeStructure, parentId, tree, stackParentId) {

			var node = new TreeNode( nodeStructure, this.db.length, parentId, tree, stackParentId );

			this.db.push( node );
			if( parentId >= 0 ) this.get( parentId ).children.push( node.id ); //skip root node

			if( stackParentId ) {
				this.get( stackParentId ).stackParent = true;
				this.get( stackParentId ).stackChildren.push( node.id );
			}

			return node;
		},

		getMinMaxCoord: function( dim, parent, MinMax ) { // used for getting the dimensions of the tree, dim = 'X' || 'Y'
			// looks for min and max (X and Y) within the set of nodes
			var parent = parent || this.get(0),
			 	i = parent.childrenCount(),
				MinMax = MinMax || { // start with root node dimensions
					min: parent[dim],
					max: parent[dim] + ((dim == 'X') ? parent.width : parent.height)
				};

			while(i--) {

				var node = parent.childAt(i),
					maxTest = node[dim] + ((dim == 'X') ? node.width : node.height),
					minTest = node[dim];

				if (maxTest > MinMax.max) {
					MinMax.max = maxTest;

				}
				if (minTest < MinMax.min) {
					MinMax.min = minTest;
				}
			
				this.getMinMaxCoord(dim, node, MinMax);
			}
			return MinMax;
		},

		hasGrandChildren: function(nodeStructure) {
			var i = nodeStructure.children.length;
			while(i--) {
				if(nodeStructure.children[i].children) return true;
			}
		}
	};


	/**
	* TreeNode constructor.
	* @constructor
	*/
	var TreeNode = function (nodeStructure, id, parentId, tree, stackParentId) {

		this.id			= id;
		this.parentId	= parentId;
		this.treeId		= tree.id;
		this.prelim		= 0;
		this.modifier	= 0;

		this.stackParentId = stackParentId;

		// pseudo node is a node with width=height=0, it is invisible, but necessary for the correct positiong of the tree
		this.pseudo = nodeStructure === 'pseudo' || nodeStructure['pseudo'];

		this.image = nodeStructure['image'];

		this.link = UTIL.createMerge( tree.CONFIG.node.link,  nodeStructure['link']);

		this.connStyle = UTIL.createMerge(tree.CONFIG.connectors, nodeStructure['connectors']);

		this.drawLineThrough = nodeStructure['drawLineThrough'] === false ? false : nodeStructure['drawLineThrough'] || tree.CONFIG.node['drawLineThrough'];
		
		this.collapsable = nodeStructure['collapsable'] === false ? false : nodeStructure['collapsable'] || tree.CONFIG.node['collapsable'];
		this.collapsed = nodeStructure['collapsed'];

		this.text = nodeStructure['text'];

		// '.node' DIV
		this.nodeInnerHTML	= nodeStructure['innerHTML'];
		this.nodeHTMLclass	= (tree.CONFIG.node['HTMLclass'] ? tree.CONFIG.node['HTMLclass'] : '') + // globaly defined class for the nodex
								(nodeStructure['HTMLclass'] ? (' ' + nodeStructure['HTMLclass']) : '');		// + specific node class

		this.nodeHTMLid		= nodeStructure['HTMLid'];
	};

	TreeNode.prototype = {

		Tree: function() {
			return TreeStore.get(this.treeId);
		},

		dbGet: function(nodeId) {
			return this.Tree().nodeDB.get(nodeId);
		},

		size: function() { // returns the width of the node
			var orient = this.Tree().CONFIG.rootOrientation;

			if(this.pseudo) return - this.Tree().CONFIG.subTeeSeparation; // prevents of separateing the subtrees

			if (orient == 'NORTH' || orient == 'SOUTH')
				return this.width;

			else if (orient == 'WEST' || orient == 'EAST')
				return this.height;
		},

		childrenCount: function () {
			return	(this.collapsed || !this.children) ? 0 : this.children.length;
		},

		childAt: function(i) {
			return this.dbGet( this.children[i] );
		},

		firstChild: function() {
			return this.childAt(0);
		},

		lastChild: function() {
			return this.childAt( this.children.length - 1 );
		},

		parent: function() {
			return this.dbGet( this.parentId );
		},

		leftNeighbor: function() {
			if( this.leftNeighborId ) return this.dbGet( this.leftNeighborId );
		},

		rightNeighbor: function() {
			if( this.rightNeighborId ) return this.dbGet( this.rightNeighborId );
		},

		leftSibling: function () {
			var leftNeighbor = this.leftNeighbor();

			if( leftNeighbor && leftNeighbor.parentId == this.parentId ) return leftNeighbor;
		},

		rightSibling: function () {
			var rightNeighbor = this.rightNeighbor();

			if( rightNeighbor && rightNeighbor.parentId == this.parentId ) return rightNeighbor;
		},

		childrenCenter: function ( tree ) {
			var first = this.firstChild(),
				last = this.lastChild();
			return first.prelim + ((last.prelim - first.prelim) + last.size()) / 2;
		},

		// find out if one of the node ancestors is collapsed
		collapsedParent: function() {
			var parent = this.parent();
			if (!parent) return false;
			if (parent.collapsed) return parent;
			return parent.collapsedParent();
		},

		leftMost: function ( level, depth ) { // returns the leftmost child at specific level, (initaial level = 0)

			if( level >= depth ) return this;
			if( this.childrenCount() === 0 ) return;

			for(var i = 0, n = this.childrenCount(); i < n; i++) {

				var leftmostDescendant = this.childAt(i).leftMost( level + 1, depth );
				if(leftmostDescendant)
					return leftmostDescendant;
			}
		},

		// returns start or the end point of the connector line, origin is upper-left
		connectorPoint: function(startPoint) {
			var orient = this.Tree().CONFIG.rootOrientation, point = {};

			if(this.stackParentId) { // return different end point if node is a stacked child
				if (orient == 'NORTH' || orient == 'SOUTH') { orient = 'WEST'; }
				else if (orient == 'EAST' || orient == 'WEST') { orient = 'NORTH'; }
			}
			// if pseudo, a virtual center is used
			if (orient == 'NORTH') {

				point.x = (this.pseudo) ? this.X - this.Tree().CONFIG.subTeeSeparation/2 : this.X + this.width/2;
				point.y = (startPoint) ? this.Y + this.height : this.Y;

			} else if (orient == 'SOUTH') {

				point.x = (this.pseudo) ? this.X - this.Tree().CONFIG.subTeeSeparation/2 : this.X + this.width/2;
				point.y = (startPoint) ? this.Y : this.Y + this.height;

			} else if (orient == 'EAST') {

				point.x = (startPoint) ? this.X : this.X + this.width;
				point.y = (this.pseudo) ? this.Y - this.Tree().CONFIG.subTeeSeparation/2 : this.Y + this.height/2;

			} else if (orient == 'WEST') {

				point.x = (startPoint) ? this.X + this.width : this.X;
				point.y =  (this.pseudo) ? this.Y - this.Tree().CONFIG.subTeeSeparation/2 : this.Y + this.height/2;
			}
			return point;
		},

		pathStringThrough: function() { // get the geometry of a path going through the node
			var startPoint = this.connectorPoint(true),
				endPoint = this.connectorPoint(false);

			return ["M", startPoint.x+","+startPoint.y, 'L', endPoint.x+","+endPoint.y].join(" ");
		},

		drawLineThroughMe: function(hidePoint) { // hidepoint se proslijedjuje ako je node sakriven zbog collapsed
			
			var pathString = hidePoint ? this.Tree().getPointPathString(hidePoint) : this.pathStringThrough();

			this.lineThroughMe = this.lineThroughMe || this.Tree()._R.path(pathString);
			
			var line_style = UTIL.cloneObj(this.connStyle.style);

			delete line_style['arrow-start'];
			delete line_style['arrow-end'];

			this.lineThroughMe.attr( line_style );

			if(hidePoint) {
				this.lineThroughMe.hide();
				this.lineThroughMe.hidden = true;
			}
		},

		addSwitchEvent: function(my_switch) {
			var self = this;
			UTIL.addEvent(my_switch, 'click', function(){
				self.toggleCollapse();
			});
		},

		toggleCollapse: function() {
			var tree = this.Tree();

			if (! tree.inAnimation) {
			
				tree.inAnimation = true;

				this.collapsed = !this.collapsed; // toglle the collapse at each click
				if (this.collapsed) {
					$(this.nodeDOM).addClass('collapsed');
				} else {
					$(this.nodeDOM).removeClass('collapsed');
				}
				tree.positionTree();
				
				setTimeout(function() { // set the flag after the animation
					tree.inAnimation = false;
				}, tree.CONFIG['animation']['nodeSpeed'] > tree.CONFIG['animation']['connectorsSpeed'] ? tree.CONFIG['animation']['nodeSpeed'] : tree.CONFIG['animation']['connectorsSpeed'])
			}
		},

		hide: function(collapse_to_point) {
			this.nodeDOM.style.overflow = "hidden";

			var jq_node = $(this.nodeDOM), tree = this.Tree(),
				config = tree.CONFIG,
				new_pos = {
					left: collapse_to_point.x,
					top: collapse_to_point.y
				};

			if (!this.hidden) { new_pos.width = new_pos.height = 0; }

			// store old width + height - padding problem when returning back to old state
			if(!this.startW || !this.startH) { this.startW = jq_node.width(); this.startH = jq_node.height(); }

			// if parent was hidden in initial configuration, position the node behind the parent without animations
			if(!this.positioned || this.hidden) {
				this.nodeDOM.style.visibility = 'hidden';
				jq_node.css(new_pos);
				this.positioned = true;
			} else {
				jq_node.animate(new_pos, config['animation']['nodeSpeed'], config['animation']['nodeAnimation'], 
				function(){
					this.style.visibility = 'hidden';
				});
			}			
			
			// animate the line through node if the line exists
			if(this.lineThroughMe) {
				var new_path = tree.getPointPathString(collapse_to_point);
				if (this.hidden) {
					// update without animations
					this.lineThroughMe.attr({path: new_path});
				} else {
					// update with animations
					tree.animatePath(this.lineThroughMe, tree.getPointPathString(collapse_to_point));
				}
			}

			this.hidden = true;
		},

		show: function() {
			this.nodeDOM.style.visibility = 'visible';

			var new_pos = {
				left: this.X,
				top: this.Y
			},
			tree = this.Tree(),  config = tree.CONFIG;

			// if the node was hidden, update width and height
			if(this.hidden) {
				new_pos['width'] = this.startW;
				new_pos['height'] = this.startH;
			}
			
			$(this.nodeDOM).animate(
				new_pos, 
				config['animation']['nodeSpeed'], config['animation']['nodeAnimation'], 
				function() {
					// $.animate applys "overflow:hidden" to the node, remove it to avoid visual problems
					this.style.overflow = "";
				}
			);

			if(this.lineThroughMe) {
				tree.animatePath(this.lineThroughMe, this.pathStringThrough());
			}

			this.hidden = false;
		}
	};

	TreeNode.prototype.createGeometry = function(tree) {

		if (this.id === 0 && tree.CONFIG.hideRootNode) {
			this.width = 0; this.height = 0;
			return;
		}

		var drawArea = tree.drawArea,
			image, i,

		/////////// CREATE NODE //////////////
		node = this.link.href ? document.createElement('a') : document.createElement('div');

		node.className = (!this.pseudo) ? TreeNode.prototype.CONFIG.nodeHTMLclass : 'pseudo';
		if(this.nodeHTMLclass && !this.pseudo) node.className += ' ' + this.nodeHTMLclass;

		if(this.nodeHTMLid) node.id = this.nodeHTMLid;

		if(this.link.href) {
			node.href = this.link.href;
			node.target = this.link.target;
		}

		/////////// CREATE innerHTML //////////////
		if (!this.pseudo) {
			if (!this.nodeInnerHTML) {

				// IMAGE
				if(this.image) {
					image = document.createElement('img');

					image.src = this.image;
					node.appendChild(image);
				}

				// TEXT
				if(this.text) {
					for(var key in this.text) {
						if(TreeNode.prototype.CONFIG.textClass[key]) {
							var text = document.createElement(this.text[key].href ? 'a' : 'p');

							// meke an <a> element if required
							if (this.text[key].href) {
								text.href = this.text[key].href;
								if (this.text[key].target) { text.target = this.text[key].target; }
							}
							//.split('&lt;br&gt;').join('<br>')
							text.className = TreeNode.prototype.CONFIG.textClass[key];
							text.appendChild(document.createTextNode(
								this.text[key].val ? this.text[key].val :
									this.text[key] instanceof Object ? "'val' param missing!" : this.text[key]
								));
							text.innerHTML = text.innerHTML.split('&lt;br&gt;').join('<br>')

							node.appendChild(text);
						}
					}
				}

			} else {

				// get some element by ID and clone its structure into a node
				if (this.nodeInnerHTML.charAt(0) === "#") {
					var elem = document.getElementById(this.nodeInnerHTML.substring(1));
					if (elem) {
						node = elem.cloneNode(true);
						node.id += "-clone";
						node.className += " node";
					} else {
						node.innerHTML = "<b> Wrong ID selector </b>";
					}
				} else {
					// insert your custom HTML into a node
					node.innerHTML = this.nodeInnerHTML;
				}
			}

			// handle collapse switch
			if (this.collapsed || (this.collapsable && this.childrenCount() && !this.stackParentId)) {
				var my_switch = document.createElement('a');
				my_switch.className = "collapse-switch";
				node.appendChild(my_switch);
				this.addSwitchEvent(my_switch);
				if (this.collapsed) { node.className += " collapsed"; }
			}
		}

		/////////// APPEND all //////////////
		drawArea.appendChild(node);

		this.width = node.offsetWidth;
		this.height = node.offsetHeight;

		this.nodeDOM = node;

		tree.imageLoader.processNode(this);
	};



	// ###########################################
	//		Expose global + default CONFIG params
	// ###########################################

	Tree.prototype.CONFIG = {
		'maxDepth': 100,
		'rootOrientation': 'NORTH', // NORTH || EAST || WEST || SOUTH
		'nodeAlign': 'CENTER', // CENTER || TOP || BOTTOM
		'levelSeparation': 30,
		'siblingSeparation': 30,
		'subTeeSeparation': 30,

		'hideRootNode': false,

		'animateOnInit': false,
		'animateOnInitDelay': 500,

		'padding': 15, // the difference is seen only when the scrollbar is shown
		'scrollbar': "native", // "native" || "fancy" || "None" (PS: "fancy" requires jquery and perfect-scrollbar)

		'connectors': {

			'type': 'curve', // 'curve' || 'step' || 'straight' || 'bCurve'
			'style': {
				'stroke': 'black'
			},
			'stackIndent': 15
		},

		'node': { // each node inherits this, it can all be overrifen in node config

			// HTMLclass: 'node',
			// drawLineThrough: false,
			// collapsable: false,
			'link': {
				'target': "_self"
			}
		},

		'animation': { // each node inherits this, it can all be overrifen in node config

			'nodeSpeed': 450,
			'nodeAnimation': "linear",
			'connectorsSpeed': 450,
			'connectorsAnimation': "linear"
		}
	};

	TreeNode.prototype.CONFIG = {
		'nodeHTMLclass': 'node',

		'textClass': {
			'name':	'node-name',
			'title':	'node-title',
			'desc':	'node-desc',
			'contact': 'node-contact'
		}
	};

	// #############################################
	// Makes a JSON chart config out of Array config
	// #############################################

	var JSOnconfig = {
		make: function( configArray ) {

			var i = configArray.length, node;

			this.jsonStructure = {
				'chart': null,
				'nodeStructure': null
			};
			//fist loop: find config, find root;
			while(i--) {
				node = configArray[i];
				if (node.hasOwnProperty('container')) {
					this.jsonStructure.chart = node;
					continue;
				}

				if (!node.hasOwnProperty('parent') && ! node.hasOwnProperty('container')) {
					this.jsonStructure.nodeStructure = node;
					node.myID = this.getID();
				}
			}

			this.findChildren(configArray);

			return this.jsonStructure;
		},

		findChildren: function(nodes) {
			var parents = [0]; // start witha a root node

			while(parents.length) {
				var parentId = parents.pop(),
					parent = this.findNode(this.jsonStructure.nodeStructure, parentId),
					i = 0, len = nodes.length,
					children = [];

				for(;i<len;i++) {
					var node = nodes[i];
					if(node.parent && (node.parent.myID == parentId)) { // skip config and root nodes

						node.myID = this.getID();

						delete node.parent;

						children.push(node);
						parents.push(node.myID);
					}
				}

				if (children.length) {
					parent['children'] = children;
				}
			}
		},

		findNode: function(node, nodeId) {
			var childrenLen, found;

			if (node.myID === nodeId) {
				return node;
			} else if (node.children) {
				childrenLen = node.children.length;
				while(childrenLen--) {
					found = this.findNode(node.children[childrenLen], nodeId);
					if(found) {
						return found;
					}
				}
			}
		},

		getID: (function() {
			var i = 0;
			return function() {
				return i++;
			};
		})()
	};

	/**
	* Chart constructor.
	* @constructor
	*/
	var Treant = function(jsonConfig, callback) {

		if (jsonConfig instanceof Array) jsonConfig = JSOnconfig.make(jsonConfig);

		var newTree = TreeStore.createTree(jsonConfig);
		newTree.positionTree(callback);
	};

	/* expose constructor globaly */ 
	window['Treant'] = Treant;
})();