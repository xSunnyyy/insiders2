// Shared sortable-table helper.
//
// HTML:
//   <th data-sort="price">Price</th>   <!-- click cycles asc -> desc -->
//   <th>Catalyst</th>                  <!-- no data-sort = not sortable -->
//
// JS:
//   makeSortable(tableEl, {
//     getRows: () => DATA.tickers,
//     setRows: (rows) => DATA.tickers = rows,
//     render:  () => render(DATA),
//     accessors: {
//       ticker: r => r.symbol,
//       price:  r => r.price,
//       ...
//     },
//   });
//
// Clicking a sortable header sorts the rows array in place and re-renders.
// Numeric values sort numerically; null / undefined sort last.

(function () {
  const _state = new WeakMap();

  function compare(a, b, dir) {
    const va = a, vb = b;
    if (va == null && vb == null) return 0;
    if (va == null) return 1;           // nulls always last
    if (vb == null) return -1;
    if (typeof va === "number" && typeof vb === "number") {
      return (va - vb) * dir;
    }
    return String(va).localeCompare(String(vb),
                                    undefined, {numeric: true}) * dir;
  }

  window.makeSortable = function (table, opts) {
    if (!table) return;
    const {getRows, setRows, render, accessors} = opts;
    const ths = table.querySelectorAll("th[data-sort]");

    ths.forEach((th) => {
      // Idempotent: skip if we've already wired this header.
      if (th.dataset.sortWired === "1") return;
      th.dataset.sortWired = "1";
      th.classList.add("sortable");
      th.addEventListener("click", () => {
        const key = th.getAttribute("data-sort");
        const accessor = accessors[key];
        if (!accessor) return;

        let s = _state.get(table) || {col: null, dir: 1};
        if (s.col === key) s.dir = -s.dir;
        else { s.col = key; s.dir = 1; }
        _state.set(table, s);

        const rows = (getRows() || []).slice();
        rows.sort((ra, rb) => compare(accessor(ra), accessor(rb), s.dir));
        setRows(rows);

        ths.forEach((h) => h.classList.remove("sort-asc", "sort-desc"));
        th.classList.add(s.dir > 0 ? "sort-asc" : "sort-desc");
        render();
      });
    });
  };
})();
