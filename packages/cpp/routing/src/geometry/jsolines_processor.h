#pragma once

#include <string>
#include <vector>

namespace routing::geometry {

// JSOlines Processor - cut isochrone bands by time step

struct JsolinesFeature
{
	double step_cost;
	std::string multipolygon_wkt;
};

std::vector<JsolinesFeature> build_jsolines_wkt(
	std::vector<double> const &surface,
	int32_t width,
	int32_t height,
	double west,
	double north,
	double step_x,
	double step_y,
	std::vector<double> const &cutoffs);

} // namespace routing::geometry
