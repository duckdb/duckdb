#include <sstream>
#include <string>

#include "dump.hh"
#include "util.hh"

using namespace std;

std::string graphml_dumper::id(struct prod *p) {
	ostringstream os;
	os << pretty_type(p) << "_" << p;
	return os.str();
}

graphml_dumper::graphml_dumper(ostream &out) : o(out) {
	o << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << endl
	  << "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" "
	  << "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
	  << "xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns "
	  << "http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">" << endl;

	o << "<key id=\"retries\" for=\"node\" "
	     "attr.name=\"retries\" attr.type=\"double\" />"
	  << endl;
	o << "<key id=\"label\" for=\"node\" "
	     "attr.name=\"label\" attr.type=\"string\" />"
	  << endl;
	o << "<key id=\"scope\" for=\"node\" "
	     "attr.name=\"scope\" attr.type=\"string\" />"
	  << endl;

	o << "<graph id=\"ast\" edgedefault=\"directed\">" << endl;
}

void graphml_dumper::visit(struct prod *p) {
	o << "<node id=\"" << id(p) << "\">";
	o << "<data key=\"retries\">" << p->retries << "</data>";
	o << "<data key=\"label\">" << pretty_type(p) << "</data>";
	o << "<data key=\"scope\">" << p->scope << "</data>";
	o << "</node>" << endl;
	if (p->pprod) {
		o << "<edge source=\"" << id(p) << "\" target=\"" << id(p->pprod) << "\"/>";
	}
	o << endl;
}

graphml_dumper::~graphml_dumper() {
	o << "</graph></graphml>" << endl;
}

void ast_logger::generated(prod &query) {
	string filename("");
	filename += "sqlsmith-";
	filename += to_string(queries);
	filename += ".xml";
	ofstream os(filename);
	graphml_dumper visitor(os);
	query.accept(&visitor);
	queries++;
}
