import bodyParser from "body-parser";
import { app, query, update, sparqlEscapeUri, errorHandler } from "mu";

app.use(bodyParser.json({ type: "*/*", limit: "50mb" }));

app.post("/delta", async (req, res) => {
  console.log("Received delta", JSON.stringify(req.body, null, 2));

  const deltas = Array.isArray(req.body)
    ? req.body
    : req.body.delta || [];

  const insertsTriples = deltas.flatMap(d => d.inserts || []).filter(
    t => t.predicate.value === "http://schema.org/reviewRating"
  );
  const deletesTriples = deltas.flatMap(d => d.deletes || []).filter(
    t => t.predicate.value === "http://schema.org/about"
  );

  for (const triple of insertsTriples) {
    const reviewUri = triple.subject.value;
    const bookUri = await findBookByReview(reviewUri);
    if (bookUri) {
      await updateRating(bookUri);
    }
  }

  for (const triple of deletesTriples) {
    if (!insertsTriples.find(t => t.subject.value === triple.subject.value)) {
      const bookUri = triple.object.value;
      await updateRating(bookUri);
    }
  }

  res.sendStatus(200);
});

/** Finds the uri of the book linked to a given review. */
async function findBookByReview(reviewUri) {
  const q = `
    PREFIX schema: <http://schema.org/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    SELECT ?book WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ${sparqlEscapeUri(reviewUri)} schema:about ?book .
      }
    } LIMIT 1
  `;
  const result = await query(q);
  return result.results.bindings[0]?.book?.value || null;
}

/** Gets all ratings for reviews about a given book. */
async function getRatingsForBook(bookUri) {
  const escapedBookUri = sparqlEscapeUri(bookUri);
  const q = `
    PREFIX schema: <http://schema.org/>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    SELECT ?rating WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ?review schema:about ${escapedBookUri} ;
                schema:reviewRating ?rating .
      }
    }
  `;
  const result = await query(q);
  return result.results.bindings
    .map(b => parseFloat(b.rating.value))
    .filter(v => !isNaN(v));
}

/** Calculates average and update the book's average rating. */
async function updateRating(bookUri) {
  try {
    const ratings = await getRatingsForBook(bookUri);
    const avg =
      ratings.length > 0
        ? ratings.reduce((a, b) => a + b, 0) / ratings.length
        : null;

    console.log(
      `Recalculating average for book ${bookUri}: ${avg ?? "no ratings"}`
    );

    const escapedBookUri = sparqlEscapeUri(bookUri);
    const deleteOldAvgQuery = `
      PREFIX schema: <http://schema.org/>
      DELETE {
        GRAPH <http://mu.semte.ch/graphs/public> {
          ${escapedBookUri} schema:averageRating ?oldAvg .
        }
      }
      WHERE {
        GRAPH <http://mu.semte.ch/graphs/public> {
          ${escapedBookUri} schema:averageRating ?oldAvg .
        }
      }
    `;

    await update(deleteOldAvgQuery);

    if (avg !== null) {
      const newAvgQuery = `
        PREFIX schema: <http://schema.org/>
        INSERT DATA {
          GRAPH <http://mu.semte.ch/graphs/public> {
            ${escapedBookUri} schema:averageRating "${avg}"^^<http://www.w3.org/2001/XMLSchema#decimal> .
          }
        }
      `;
      await update(newAvgQuery);
    }

    console.log(`Updated book ${bookUri} average rating to ${avg}`);
  } catch (error) {
    const errorMessage = `Error updating average for book ${bookUri}: ${error.message}`;
    new Error(errorMessage);
    console.error(errorMessage);
  }
}

app.use(errorHandler);
app.listen(3000, () => console.log("rating-service listening on port 3000"));
