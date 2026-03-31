#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <chrono>

#include "pipeline.h"
#include "types.h"

namespace py = pybind11;

PYBIND11_MODULE(_routing, m)
{
    m.doc() = "C++ routing engine with Python bindings";

    auto elapsed_ms = [](std::chrono::steady_clock::time_point start,
                         std::chrono::steady_clock::time_point end) -> double
    {
        return std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
                   end - start)
            .count();
    };

    py::enum_<routing::RoutingMode>(m, "RoutingMode")
        .value("Walking", routing::RoutingMode::Walking)
        .value("Bicycle", routing::RoutingMode::Bicycle)
        .value("Pedelec", routing::RoutingMode::Pedelec)
        .value("Car", routing::RoutingMode::Car)
        .value("PublicTransport", routing::RoutingMode::PublicTransport);

    py::enum_<routing::CostMode>(m, "CostMode")
        .value("Time", routing::CostMode::Time)
        .value("Distance", routing::CostMode::Distance);

    py::enum_<routing::CatchmentType>(m, "CatchmentType")
        .value("Polygon", routing::CatchmentType::Polygon)
        .value("Network", routing::CatchmentType::Network)
        .value("HexagonalGrid", routing::CatchmentType::HexagonalGrid);

    py::enum_<routing::OutputFormat>(m, "OutputFormat")
        .value("GeoJSON", routing::OutputFormat::GeoJSON)
        .value("Parquet", routing::OutputFormat::Parquet);

    py::class_<routing::Point3857>(m, "Point3857")
        .def(py::init<double, double>())
        .def_readwrite("x", &routing::Point3857::x)
        .def_readwrite("y", &routing::Point3857::y);

    py::class_<routing::RequestConfig>(m, "RequestConfig")
        .def(py::init<>())
        .def_readwrite("starting_points",
                       &routing::RequestConfig::starting_points)
        .def_readwrite("mode", &routing::RequestConfig::mode)
        .def_readwrite("cost_mode", &routing::RequestConfig::cost_mode)
        .def_readwrite("max_traveltime",
                       &routing::RequestConfig::max_traveltime)
        .def_readwrite("max_distance", &routing::RequestConfig::max_distance)
        .def_readwrite("steps", &routing::RequestConfig::steps)
        .def_readwrite("speed_km_h", &routing::RequestConfig::speed_km_h)
        .def_readwrite("edge_dir", &routing::RequestConfig::edge_dir)
        .def_readwrite("timetable_path", &routing::RequestConfig::timetable_path)
        .def_readwrite("output_path", &routing::RequestConfig::output_path)
        .def_readwrite("catchment_type", &routing::RequestConfig::catchment_type)
        .def_readwrite("output_format", &routing::RequestConfig::output_format)
        .def_readwrite("polygon_difference",
                       &routing::RequestConfig::polygon_difference)
        .def_readwrite("departure_time", &routing::RequestConfig::departure_time)
        .def_readwrite("max_transfers", &routing::RequestConfig::max_transfers)
        // PT access/egress
        .def_readwrite("access_mode", &routing::RequestConfig::access_mode)
        .def_readwrite("egress_mode", &routing::RequestConfig::egress_mode)
        .def_readwrite("access_max_time", &routing::RequestConfig::access_max_time)
        .def_readwrite("egress_max_time", &routing::RequestConfig::egress_max_time)
        .def_readwrite("access_speed_km_h",
                       &routing::RequestConfig::access_speed_km_h)
        .def_readwrite("egress_speed_km_h",
                       &routing::RequestConfig::egress_speed_km_h)
        // Transit mode filter and explicit cutoffs
        .def_readwrite("transit_modes", &routing::RequestConfig::transit_modes)
        .def_readwrite("cutoffs", &routing::RequestConfig::cutoffs);

    m.def("compute_catchment",
          [elapsed_ms](routing::RequestConfig const &config)
          {
              auto t0 = std::chrono::steady_clock::now();
              auto result = routing::compute_catchment(config);
              auto t1 = std::chrono::steady_clock::now();
              py::print("[routing] compute_catchment total_ms=",
                        elapsed_ms(t0, t1));
              return result;
          },
          py::arg("config"),
          "Run the full routing pipeline and dispatch output by RequestConfig.output_format");
}
