using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using Npgsql;

namespace PostgisTileServer.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TileController : ControllerBase
    {
        private const string host = "localhost";
        private const string port = "5432";
        private const string user = "postgres";
        private const string pass = "123456";
        private const string database = "postgres";
        private const string app = "PostgisTileServer";
        private readonly TimeSpan cacheExpiration = TimeSpan.FromSeconds(60);

        private T QueryDB<T>(string query)
        {
            var connection = new NpgsqlConnection($"Host={host};Port={port};Username={user};Password={pass};Database={database}; Application Name={app}");
            
            connection.Open();
            using var cmd = new NpgsqlCommand(query, connection);
            var result = (T) cmd.ExecuteScalar();
            connection.Close();
            
            return result;
        }

        /// <summary>
        /// Perform a simple query on a table.
        /// </summary>
        /// <param name="tables">name of the tables</param>
        /// <param name="filter">Filtering parameters for a SQL WHERE statement.</param>
        /// <param name="columns">The fields to return. The default is "*".</param>
        /// <param name="limit">Limit the number of features returned. The default is 0 (unlimited)</param>
        /// <returns></returns>
        [HttpGet, Route("/json/v1/{tables}")]
        public IActionResult Json([FromServices] IMemoryCache cache, string tables, string filter, string columns = "*", int limit = 0)
        {
            try
            {
                var result = cache.GetOrCreate("JSON" + columns + tables + filter + limit, context =>
                {
                    context.SetAbsoluteExpiration(cacheExpiration);
                    var query = @$"SELECT {columns} FROM {tables} {(string.IsNullOrEmpty(filter) ? "" : $"WHERE {filter}")} {(limit == 0 ? "" : $"LIMIT {limit}")}";
                    return QueryDB<string>($"SELECT json_agg(q) FROM ({query}) AS q");
                });

                return Content(result, "application/json");
            }
            catch (Exception e)
            {
                return StatusCode(500, e.Message);
            }
        }

        /// <summary>
        /// Mapbox Vector Tile
        /// </summary>
        /// <param name="z">Z of the Z/X/Y tile spec.</param>
        /// <param name="x">X of the Z/X/Y tile spec.</param>
        /// <param name="y">Y of the Z/X/Y tile spec.</param>
        /// <param name="table">Name of the table.</param>
        /// <param name="geom_column">The geometry column of the table. The default is "geom".</param>
        /// <param name="filter">Filtering parameters for a SQL WHERE statement.</param>
        /// <param name="columns">The fields to return. The default is "*".</param>
        /// <param name="limit">Limit the number of features returned. The default is 0 (unlimited)</param>
        /// <returns>Return Mapbox Vector Tile as protobuf.</returns>
        [HttpGet, Route("/mvt/v1/{table}/{z}/{x}/{y}")]
        public IActionResult Mvt([FromServices] IMemoryCache cache, int z, int x, int y, string table, string filter, string geom_column = "geom", string columns = "", int limit = 0)
        {
            try
            {
                var result = cache.GetOrCreate<byte[]>("MVT" + x + y + z + columns + geom_column + table + filter + limit, context =>
                    {
                        context.SetAbsoluteExpiration(cacheExpiration);
                        var query = @$"SELECT {(string.IsNullOrEmpty(columns) ? "" : columns + ",")} ST_AsMVTGeom(ST_Simplify(ST_Transform({geom_column}, 3857), 0.0005), ST_SetSRID(ST_TileEnvelope({z}, {x}, {y}), 3857), 4096, 0, false) geom
FROM {table}
{(string.IsNullOrEmpty(filter) ? "" : $"WHERE {filter}")}
{(limit == 0 ? "" : $"LIMIT {limit}")}";

                        return QueryDB<byte[]>($"SELECT ST_AsMVT(q, '{table}', 4096, 'geom') FROM ({query}) AS q");
                    });

                return File(result, "application/x-protobuf");
            }
            catch (Exception e)
            {
                return StatusCode(500, e.Message);
            }
        }
    }
}
