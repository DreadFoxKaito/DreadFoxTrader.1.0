(function () {
  const canvas = document.getElementById("positions_solid_canvas");
  const faceSelect = document.getElementById("positions_solid_face_select");
  const faceCustomInput = document.getElementById("positions_solid_face_custom_input");
  const scaleModeSelect = document.getElementById("positions_solid_scale_mode");
  const refreshBtn = document.getElementById("positions_solid_refresh");
  const resetOrderBtn = document.getElementById("positions_solid_reset_order");
  const summaryEl = document.getElementById("positions_solid_summary");
  const statusEl = document.getElementById("positions_solid_status");
  const topListEl = document.getElementById("positions_solid_toplist");
  const scaleHintEl = document.getElementById("positions_solid_scale_hint");
  if (!canvas || !faceSelect || !summaryEl || !statusEl || !topListEl) {
    return;
  }

  const FACE_LEVELS = [4, 6, 8, 12, 20];
  const EPS = 1e-7;
  const STELLATION_SCALE = 0.92;
  const BASE_RADIUS = 1.55;
  const CAMERA_Z = 5.0;
  const ORDER_STORAGE_KEY = "portfolio_solid_manual_order_v1";
  const SCALE_MODE_STORAGE_KEY = "portfolio_solid_scale_mode_v1";
  const FACE_CUSTOM_STORAGE_KEY = "portfolio_solid_face_custom_v1";
  const MAX_FACE_REQUEST = 2000;

  const SOLIDS = {
    4: { name: "Tetrahedron", tint: [44, 238, 255], builder: tetraVertices, faceSize: 3 },
    6: { name: "Cube", tint: [255, 149, 52], builder: cubeVertices, faceSize: 4 },
    8: { name: "Octahedron", tint: [110, 255, 145], builder: octaVertices, faceSize: 3 },
    12: { name: "Dodecahedron", tint: [255, 119, 255], builder: dodecaVertices, faceSize: 5 },
    20: { name: "Icosahedron", tint: [255, 237, 72], builder: icosaVertices, faceSize: 3 },
  };

  const state = {
    ctx: canvas.getContext("2d"),
    dpr: 1,
    width: 0,
    height: 0,
    angleX: 0.46,
    angleY: 0.12,
    currentFaceCount: 4,
    currentSolidName: "Tetrahedron",
    solid: buildStellatedSolid(4),
    rawPositions: [],
    positions: [],
    manualOrderSymbols: [],
    spikeScaleMode: "weight",
    currentFaceRequest: 4,
    mappedFaceIndices: [],
    totalPositions: 0,
    autoFaceCount: 4,
    pendingFetch: false,
    lastUpdated: 0,
    refreshHandle: null,
    frameHandle: null,
  };

  function vec(x, y, z) {
    return { x: x, y: y, z: z };
  }

  function add(a, b) {
    return vec(a.x + b.x, a.y + b.y, a.z + b.z);
  }

  function sub(a, b) {
    return vec(a.x - b.x, a.y - b.y, a.z - b.z);
  }

  function mul(a, k) {
    return vec(a.x * k, a.y * k, a.z * k);
  }

  function dot(a, b) {
    return a.x * b.x + a.y * b.y + a.z * b.z;
  }

  function cross(a, b) {
    return vec(
      a.y * b.z - a.z * b.y,
      a.z * b.x - a.x * b.z,
      a.x * b.y - a.y * b.x
    );
  }

  function mag(a) {
    return Math.sqrt(dot(a, a));
  }

  function norm(a) {
    const m = mag(a);
    if (m < EPS) {
      return vec(0, 0, 0);
    }
    return mul(a, 1 / m);
  }

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  function normalizeSymbol(value) {
    return String(value || "").trim().toUpperCase();
  }

  function normalizeScaleMode(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === "gain_loss") {
      return "gain_loss";
    }
    return "weight";
  }

  function normalizeFaceRequest(value) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) {
      return null;
    }
    return Math.max(4, Math.min(MAX_FACE_REQUEST, Math.round(parsed)));
  }

  function loadCustomFaceRequest() {
    let resolved = normalizeFaceRequest(faceCustomInput && faceCustomInput.value);
    try {
      if (window.localStorage) {
        const stored = window.localStorage.getItem(FACE_CUSTOM_STORAGE_KEY);
        const storedValue = normalizeFaceRequest(stored);
        if (storedValue !== null) {
          resolved = storedValue;
        }
      }
    } catch (_err) {
      // Ignore storage read failures.
    }
    if (resolved === null) {
      resolved = 40;
    }
    if (faceCustomInput) {
      faceCustomInput.value = String(resolved);
    }
  }

  function saveCustomFaceRequest() {
    if (!faceCustomInput) {
      return;
    }
    const resolved = normalizeFaceRequest(faceCustomInput.value);
    if (resolved === null) {
      return;
    }
    faceCustomInput.value = String(resolved);
    try {
      if (!window.localStorage) {
        return;
      }
      window.localStorage.setItem(FACE_CUSTOM_STORAGE_KEY, String(resolved));
    } catch (_err) {
      // Ignore storage write failures.
    }
  }

  function isCustomFaceMode() {
    return String(faceSelect && faceSelect.value || "").trim().toLowerCase() === "custom";
  }

  function syncFaceCustomInputState() {
    if (!faceCustomInput) {
      return;
    }
    const enabled = isCustomFaceMode();
    faceCustomInput.disabled = !enabled;
    faceCustomInput.style.opacity = enabled ? "1" : "0.6";
  }

  function loadScaleMode() {
    let resolved = normalizeScaleMode(scaleModeSelect && scaleModeSelect.value);
    try {
      if (window.localStorage) {
        const stored = window.localStorage.getItem(SCALE_MODE_STORAGE_KEY);
        if (stored) {
          resolved = normalizeScaleMode(stored);
        }
      }
    } catch (_err) {
      // Ignore storage read failures.
    }
    state.spikeScaleMode = resolved;
    if (scaleModeSelect) {
      scaleModeSelect.value = resolved;
    }
  }

  function saveScaleMode() {
    try {
      if (!window.localStorage) {
        return;
      }
      window.localStorage.setItem(SCALE_MODE_STORAGE_KEY, state.spikeScaleMode);
    } catch (_err) {
      // Ignore storage write failures.
    }
  }

  function loadManualOrder() {
    try {
      if (!window.localStorage) {
        return;
      }
      const raw = window.localStorage.getItem(ORDER_STORAGE_KEY);
      if (!raw) {
        return;
      }
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) {
        return;
      }
      state.manualOrderSymbols = parsed
        .map(normalizeSymbol)
        .filter(function (sym) {
          return !!sym;
        });
    } catch (_err) {
      state.manualOrderSymbols = [];
    }
  }

  function saveManualOrder() {
    try {
      if (!window.localStorage) {
        return;
      }
      const list = state.manualOrderSymbols
        .map(normalizeSymbol)
        .filter(function (sym) {
          return !!sym;
        });
      if (!list.length) {
        window.localStorage.removeItem(ORDER_STORAGE_KEY);
        return;
      }
      window.localStorage.setItem(ORDER_STORAGE_KEY, JSON.stringify(list));
    } catch (_err) {
      // Ignore storage write failures.
    }
  }

  function applyManualOrder(rows) {
    const rankedRows = Array.isArray(rows) ? rows.slice() : [];
    if (!rankedRows.length || !state.manualOrderSymbols.length) {
      return rankedRows;
    }
    const bySymbol = new Map();
    rankedRows.forEach(function (row) {
      const sym = normalizeSymbol(row && row.symbol);
      if (!sym || bySymbol.has(sym)) {
        return;
      }
      bySymbol.set(sym, row);
    });

    const output = [];
    const used = new Set();
    state.manualOrderSymbols.forEach(function (symRaw) {
      const sym = normalizeSymbol(symRaw);
      if (!sym || used.has(sym)) {
        return;
      }
      const row = bySymbol.get(sym);
      if (!row) {
        return;
      }
      output.push(row);
      used.add(sym);
    });

    rankedRows.forEach(function (row) {
      const sym = normalizeSymbol(row && row.symbol);
      if (!sym || used.has(sym)) {
        return;
      }
      output.push(row);
      used.add(sym);
    });

    return output;
  }

  function captureManualOrderFromCurrent() {
    state.manualOrderSymbols = state.positions
      .map(function (row) {
        return normalizeSymbol(row && row.symbol);
      })
      .filter(function (sym) {
        return !!sym;
      });
    saveManualOrder();
  }

  function moveSymbol(symbol, direction) {
    const sym = normalizeSymbol(symbol);
    if (!sym || !Array.isArray(state.positions) || !state.positions.length) {
      return false;
    }
    const idx = state.positions.findIndex(function (row) {
      return normalizeSymbol(row && row.symbol) === sym;
    });
    if (idx < 0) {
      return false;
    }
    const targetIdx = direction === "up" ? idx - 1 : idx + 1;
    if (targetIdx < 0 || targetIdx >= state.positions.length) {
      return false;
    }
    const next = state.positions.slice();
    const tmp = next[idx];
    next[idx] = next[targetIdx];
    next[targetIdx] = tmp;
    state.positions = next;
    captureManualOrderFromCurrent();
    return true;
  }

  function tetraVertices() {
    return [
      vec(1, 1, 1),
      vec(-1, -1, 1),
      vec(-1, 1, -1),
      vec(1, -1, -1),
    ];
  }

  function cubeVertices() {
    return [
      vec(-1, -1, -1),
      vec(1, -1, -1),
      vec(1, 1, -1),
      vec(-1, 1, -1),
      vec(-1, -1, 1),
      vec(1, -1, 1),
      vec(1, 1, 1),
      vec(-1, 1, 1),
    ];
  }

  function octaVertices() {
    return [
      vec(1, 0, 0),
      vec(-1, 0, 0),
      vec(0, 1, 0),
      vec(0, -1, 0),
      vec(0, 0, 1),
      vec(0, 0, -1),
    ];
  }

  function dodecaVertices() {
    const phi = (1 + Math.sqrt(5)) / 2;
    const invPhi = 1 / phi;
    const points = [];
    [-1, 1].forEach(function (sx) {
      [-1, 1].forEach(function (sy) {
        [-1, 1].forEach(function (sz) {
          points.push(vec(sx, sy, sz));
        });
      });
    });
    [-1, 1].forEach(function (sy) {
      [-1, 1].forEach(function (sz) {
        points.push(vec(0, sy * invPhi, sz * phi));
      });
    });
    [-1, 1].forEach(function (sx) {
      [-1, 1].forEach(function (sy) {
        points.push(vec(sx * invPhi, sy * phi, 0));
      });
    });
    [-1, 1].forEach(function (sx) {
      [-1, 1].forEach(function (sz) {
        points.push(vec(sx * phi, 0, sz * invPhi));
      });
    });
    return points;
  }

  function icosaVertices() {
    const phi = (1 + Math.sqrt(5)) / 2;
    const points = [];
    [-1, 1].forEach(function (sy) {
      [-1, 1].forEach(function (sz) {
        points.push(vec(0, sy, sz * phi));
      });
    });
    [-1, 1].forEach(function (sx) {
      [-1, 1].forEach(function (sy) {
        points.push(vec(sx, sy * phi, 0));
      });
    });
    [-1, 1].forEach(function (sx) {
      [-1, 1].forEach(function (sz) {
        points.push(vec(sx * phi, 0, sz));
      });
    });
    return points;
  }

  function normalizeVertices(points) {
    let maxRadius = 0;
    points.forEach(function (p) {
      const m = mag(p);
      if (m > maxRadius) {
        maxRadius = m;
      }
    });
    const scale = maxRadius > EPS ? BASE_RADIUS / maxRadius : 1;
    return points.map(function (p) {
      return mul(p, scale);
    });
  }

  function planeKey(normal, offset) {
    return [
      normal.x.toFixed(6),
      normal.y.toFixed(6),
      normal.z.toFixed(6),
      offset.toFixed(6),
    ].join("|");
  }

  function orderedFaceIndices(indices, vertices, normal) {
    let center = vec(0, 0, 0);
    indices.forEach(function (idx) {
      center = add(center, vertices[idx]);
    });
    center = mul(center, 1 / indices.length);

    let axisU = norm(sub(vertices[indices[0]], center));
    let axisV = cross(normal, axisU);
    if (mag(axisV) < EPS && indices.length > 1) {
      axisU = norm(sub(vertices[indices[1]], center));
      axisV = cross(normal, axisU);
    }
    axisV = norm(axisV);

    const withAngles = indices.map(function (idx) {
      const rel = sub(vertices[idx], center);
      const angle = Math.atan2(dot(rel, axisV), dot(rel, axisU));
      return { idx: idx, angle: angle };
    });

    withAngles.sort(function (a, b) {
      return a.angle - b.angle;
    });
    return withAngles.map(function (x) {
      return x.idx;
    });
  }

  function extractFaces(vertices) {
    const count = vertices.length;
    let center = vec(0, 0, 0);
    vertices.forEach(function (p) {
      center = add(center, p);
    });
    center = mul(center, 1 / Math.max(1, count));

    const facePlanes = new Map();

    for (let i = 0; i < count - 2; i += 1) {
      for (let j = i + 1; j < count - 1; j += 1) {
        for (let k = j + 1; k < count; k += 1) {
          const a = vertices[i];
          const b = vertices[j];
          const c = vertices[k];
          let normal = cross(sub(b, a), sub(c, a));
          if (mag(normal) < EPS) {
            continue;
          }

          let hasPositive = false;
          let hasNegative = false;
          for (let m = 0; m < count; m += 1) {
            if (m === i || m === j || m === k) {
              continue;
            }
            const dist = dot(normal, sub(vertices[m], a));
            if (dist > EPS) {
              hasPositive = true;
            } else if (dist < -EPS) {
              hasNegative = true;
            }
            if (hasPositive && hasNegative) {
              break;
            }
          }
          if (hasPositive && hasNegative) {
            continue;
          }

          normal = norm(normal);
          if (dot(normal, sub(a, center)) < 0) {
            normal = mul(normal, -1);
          }
          const offset = dot(normal, a);
          const key = planeKey(normal, offset);
          if (!facePlanes.has(key)) {
            facePlanes.set(key, { normal: normal, indices: new Set() });
          }
          const entry = facePlanes.get(key);
          for (let m = 0; m < count; m += 1) {
            if (Math.abs(dot(normal, vertices[m]) - offset) < 1e-5) {
              entry.indices.add(m);
            }
          }
        }
      }
    }

    const faces = [];
    facePlanes.forEach(function (entry) {
      const indices = Array.from(entry.indices);
      if (indices.length < 3) {
        return;
      }
      faces.push(orderedFaceIndices(indices, vertices, entry.normal));
    });
    return faces;
  }

  function edgeKey(a, b) {
    const k1 = [a.x.toFixed(6), a.y.toFixed(6), a.z.toFixed(6)].join(",");
    const k2 = [b.x.toFixed(6), b.y.toFixed(6), b.z.toFixed(6)].join(",");
    if (k1 < k2) {
      return k1 + "|" + k2;
    }
    return k2 + "|" + k1;
  }

  function canonicalFaceKey(indices) {
    return (Array.isArray(indices) ? indices.slice() : [])
      .sort(function (a, b) {
        return a - b;
      })
      .join(",");
  }

  function dedupeFaces(faces) {
    const unique = [];
    const seen = new Set();
    (Array.isArray(faces) ? faces : []).forEach(function (face) {
      if (!Array.isArray(face) || face.length < 3) {
        return;
      }
      const key = canonicalFaceKey(face);
      if (!key || seen.has(key)) {
        return;
      }
      seen.add(key);
      unique.push(face);
    });
    return unique;
  }

  function chooseGeodesicFrequency(requestedFaceCount) {
    const requested = Math.max(21, Number(requestedFaceCount) || 21);
    return Math.max(2, Math.ceil(Math.sqrt(requested / 20)));
  }

  function geodesicIcosahedron(frequency) {
    const freq = Math.max(2, Math.round(Number(frequency) || 2));
    const baseVertices = icosaVertices().map(function (v) {
      return norm(v);
    });
    const baseFaces = dedupeFaces(extractFaces(baseVertices)).filter(function (face) {
      return Array.isArray(face) && face.length === 3;
    });

    const uniqueVertices = new Map();
    const vertices = [];
    const faces = [];

    function getVertexIndex(point) {
      const scaled = mul(norm(point), BASE_RADIUS);
      const key = [scaled.x.toFixed(7), scaled.y.toFixed(7), scaled.z.toFixed(7)].join("|");
      if (uniqueVertices.has(key)) {
        return uniqueVertices.get(key);
      }
      const idx = vertices.length;
      uniqueVertices.set(key, idx);
      vertices.push(scaled);
      return idx;
    }

    baseFaces.forEach(function (face) {
      const a = baseVertices[face[0]];
      const b = baseVertices[face[1]];
      const c = baseVertices[face[2]];
      const grid = [];

      for (let i = 0; i <= freq; i += 1) {
        const row = [];
        for (let j = 0; j <= (freq - i); j += 1) {
          const k = freq - i - j;
          const point = add(
            add(mul(a, i / freq), mul(b, j / freq)),
            mul(c, k / freq)
          );
          row.push(getVertexIndex(point));
        }
        grid.push(row);
      }

      for (let i = 0; i < freq; i += 1) {
        for (let j = 0; j < (freq - i); j += 1) {
          const v0 = grid[i][j];
          const v1 = grid[i + 1][j];
          const v2 = grid[i][j + 1];
          faces.push([v0, v1, v2]);
          if (j < (freq - i - 1)) {
            const v3 = grid[i + 1][j + 1];
            faces.push([v1, v3, v2]);
          }
        }
      }
    });

    return { vertices: vertices, faces: faces };
  }

  function buildStellatedSolid(faceCount) {
    const requested = Math.max(4, Number(faceCount) || 4);
    let solidName = "Icosahedron";
    let tint = [255, 237, 72];
    let baseVertices = [];
    let faces = [];

    if (requested > FACE_LEVELS[FACE_LEVELS.length - 1]) {
      const frequency = chooseGeodesicFrequency(requested);
      const generated = geodesicIcosahedron(frequency);
      baseVertices = generated.vertices;
      faces = generated.faces;
      solidName = "Geodesic Icosahedron f=" + frequency;
      tint = [44, 238, 255];
    } else {
      const chosenFaceCount = chooseFaceLevel(requested);
      const solidConfig = SOLIDS[chosenFaceCount] || SOLIDS[20];
      baseVertices = normalizeVertices(solidConfig.builder());
      faces = dedupeFaces(extractFaces(baseVertices));
      const expectedFaceSize = Number(solidConfig.faceSize);
      if (Number.isFinite(expectedFaceSize) && expectedFaceSize >= 3) {
        const sizeMatched = faces.filter(function (face) {
          return Array.isArray(face) && face.length === expectedFaceSize;
        });
        if (sizeMatched.length) {
          faces = sizeMatched;
        }
      }
      solidName = solidConfig.name;
      tint = solidConfig.tint;
    }
    const spikes = [];
    const baseEdges = [];
    const seenBaseEdges = new Set();

    faces.forEach(function (face, faceIndex) {
      const points = face.map(function (idx) {
        return baseVertices[idx];
      });
      if (points.length < 3) {
        return;
      }

      let center = vec(0, 0, 0);
      points.forEach(function (p) {
        center = add(center, p);
      });
      center = mul(center, 1 / points.length);

      let normal = norm(cross(sub(points[1], points[0]), sub(points[2], points[0])));
      if (dot(normal, center) < 0) {
        normal = mul(normal, -1);
      }

      let faceRadius = 0;
      points.forEach(function (p) {
        faceRadius += mag(sub(p, center));
      });
      faceRadius /= points.length;

      const shade = [0.86, 1.0, 1.16][faceIndex % 3];
      spikes.push({
        points: points,
        center: center,
        normal: normal,
        faceRadius: faceRadius,
        shade: shade,
      });

      for (let i = 0; i < points.length; i += 1) {
        const next = points[(i + 1) % points.length];
        const baseKey = edgeKey(points[i], next);
        if (!seenBaseEdges.has(baseKey)) {
          seenBaseEdges.add(baseKey);
          baseEdges.push({ a: points[i], b: next });
        }
      }
    });

    return {
      requestedFaceCount: requested,
      faceCount: faces.length,
      name: solidName,
      tint: tint,
      spikes: spikes,
      baseEdges: baseEdges,
    };
  }

  function chooseFaceLevel(requestedCount) {
    const count = Math.max(0, Number(requestedCount) || 0);
    for (let i = 0; i < FACE_LEVELS.length; i += 1) {
      if (count <= FACE_LEVELS[i]) {
        return FACE_LEVELS[i];
      }
    }
    return FACE_LEVELS[FACE_LEVELS.length - 1];
  }

  function rotatePoint(point, angleX, angleY) {
    const cosX = Math.cos(angleX);
    const sinX = Math.sin(angleX);
    const cosY = Math.cos(angleY);
    const sinY = Math.sin(angleY);

    const y1 = point.y * cosX - point.z * sinX;
    const z1 = point.y * sinX + point.z * cosX;
    const x2 = point.x * cosY + z1 * sinY;
    const z2 = -point.x * sinY + z1 * cosY;
    return vec(x2, y1, z2);
  }

  function project(point, width, height) {
    const depth = CAMERA_Z - point.z;
    const safeDepth = Math.max(0.22, depth);
    const scale = (Math.min(width, height) * 0.9) / safeDepth;
    return {
      x: width * 0.5 + point.x * scale,
      y: height * 0.5 - point.y * scale,
      z: point.z,
      depth: depth,
    };
  }

  function drawRoundedRect(ctx, x, y, w, h, r) {
    const radius = Math.max(0, Math.min(r, Math.min(w, h) / 2));
    ctx.beginPath();
    ctx.moveTo(x + radius, y);
    ctx.lineTo(x + w - radius, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + radius);
    ctx.lineTo(x + w, y + h - radius);
    ctx.quadraticCurveTo(x + w, y + h, x + w - radius, y + h);
    ctx.lineTo(x + radius, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - radius);
    ctx.lineTo(x, y + radius);
    ctx.quadraticCurveTo(x, y, x + radius, y);
    ctx.closePath();
  }

  function positionStrength(position) {
    if (!position || typeof position !== "object") {
      return 0;
    }
    const weight = Number(position.weight);
    if (Number.isFinite(weight) && weight > 0) {
      return weight;
    }
    const rankMetric = Number(position.rank_metric);
    if (Number.isFinite(rankMetric) && rankMetric > 0) {
      return rankMetric;
    }
    const mv = Number(position.market_value_abs);
    if (Number.isFinite(mv) && mv > 0) {
      return mv;
    }
    const qty = Number(position.quantity_abs);
    if (Number.isFinite(qty) && qty > 0) {
      return qty;
    }
    return 0;
  }

  function positionGainLossPercent(position) {
    if (!position || typeof position !== "object") {
      return Number.NaN;
    }
    const pct = Number(position.gain_loss_percent);
    if (Number.isFinite(pct)) {
      return pct;
    }
    const plDollar = Number(position.gain_loss_dollar);
    const basisSigned = Number(position.cost_basis_signed);
    if (Number.isFinite(plDollar) && Number.isFinite(basisSigned) && Math.abs(basisSigned) > 1e-9) {
      return plDollar / Math.abs(basisSigned);
    }
    return Number.NaN;
  }

  function computeWeightSpikeScales(mappedPositions, faceSlots) {
    const count = Math.max(0, Number(faceSlots) || 0);
    const scales = new Array(count).fill(0.58);
    if (!count) {
      return scales;
    }

    const used = (Array.isArray(mappedPositions) ? mappedPositions : []).slice(0, count);
    if (!used.length) {
      return scales.map(function () {
        return 1.0;
      });
    }

    const strengths = used.map(positionStrength);
    const positive = strengths.filter(function (v) {
      return Number.isFinite(v) && v > 0;
    });
    if (!positive.length) {
      for (let i = 0; i < used.length; i += 1) {
        scales[i] = 1.0;
      }
      return scales;
    }

    const strengthSum = positive.reduce(function (acc, cur) {
      return acc + cur;
    }, 0);
    const uniformShare = 1 / used.length;

    for (let i = 0; i < used.length; i += 1) {
      const strength = strengths[i];
      if (!Number.isFinite(strength) || strength <= 0 || strengthSum <= 0) {
        scales[i] = 0.62;
        continue;
      }
      const share = strength / strengthSum;
      const relative = share / uniformShare;
      // Scale spike depth by relative portfolio share vs. equally weighted faces.
      scales[i] = clamp(0.55 + relative * 0.65, 0.45, 2.2);
    }
    return scales;
  }

  function computeGainLossSpikeScales(mappedPositions, faceSlots) {
    const count = Math.max(0, Number(faceSlots) || 0);
    const scales = new Array(count).fill(1.0);
    if (!count) {
      return scales;
    }

    const used = (Array.isArray(mappedPositions) ? mappedPositions : []).slice(0, count);
    if (!used.length) {
      return scales;
    }

    const pctSignals = used.map(function (position) {
      const pct = positionGainLossPercent(position);
      return Number.isFinite(pct) ? pct : 0;
    });
    const maxAbs = pctSignals.reduce(function (acc, value) {
      const v = Math.abs(value);
      return v > acc ? v : acc;
    }, 0);
    if (maxAbs <= 1e-9) {
      return scales;
    }

    for (let i = 0; i < used.length; i += 1) {
      const normalized = clamp(pctSignals[i] / maxAbs, -1, 1);
      if (normalized >= 0) {
        scales[i] = clamp(1.0 + normalized * 1.2, 0.45, 2.2);
      } else {
        scales[i] = clamp(1.0 + normalized * 0.55, 0.45, 2.2);
      }
    }
    return scales;
  }

  function computeSpikeScales(mappedPositions, faceSlots) {
    if (state.spikeScaleMode === "gain_loss") {
      return computeGainLossSpikeScales(mappedPositions, faceSlots);
    }
    return computeWeightSpikeScales(mappedPositions, faceSlots);
  }

  function faceProfitSignal(position) {
    if (!position || typeof position !== "object") {
      return 0;
    }
    const plDollar = Number(position.gain_loss_dollar);
    if (Number.isFinite(plDollar) && Math.abs(plDollar) > 1e-9) {
      return plDollar;
    }
    const plPercent = positionGainLossPercent(position);
    if (Number.isFinite(plPercent) && Math.abs(plPercent) > 1e-9) {
      return plPercent;
    }
    return 0;
  }

  function faceTintForPosition(position, baseTint) {
    const base = Array.isArray(baseTint) && baseTint.length === 3 ? baseTint : [185, 203, 226];
    const signal = faceProfitSignal(position);
    if (!Number.isFinite(signal) || Math.abs(signal) <= 1e-9) {
      return base;
    }

    const target = signal > 0 ? [72, 255, 106] : [255, 84, 84];
    const pct = positionGainLossPercent(position);
    const intensity = Number.isFinite(pct)
      ? clamp(0.62 + Math.abs(pct) * 3.2, 0.62, 1.0)
      : 0.88;

    return [
      Math.round(base[0] * (1 - intensity) + target[0] * intensity),
      Math.round(base[1] * (1 - intensity) + target[1] * intensity),
      Math.round(base[2] * (1 - intensity) + target[2] * intensity),
    ];
  }

  function faceCountFromSelector() {
    const raw = String(faceSelect.value || "auto").trim().toLowerCase();
    if (raw === "auto") {
      return chooseFaceLevel(state.totalPositions);
    }
    if (raw === "custom") {
      const parsedCustom = normalizeFaceRequest(faceCustomInput && faceCustomInput.value);
      if (parsedCustom !== null) {
        if (faceCustomInput) {
          faceCustomInput.value = String(parsedCustom);
        }
        return parsedCustom;
      }
      return 20;
    }
    const parsed = Number(raw);
    if (FACE_LEVELS.indexOf(parsed) >= 0) {
      return parsed;
    }
    return chooseFaceLevel(state.totalPositions);
  }

  function distanceSquared(a, b) {
    const dx = (a && Number.isFinite(a.x) ? a.x : 0) - (b && Number.isFinite(b.x) ? b.x : 0);
    const dy = (a && Number.isFinite(a.y) ? a.y : 0) - (b && Number.isFinite(b.y) ? b.y : 0);
    const dz = (a && Number.isFinite(a.z) ? a.z : 0) - (b && Number.isFinite(b.z) ? b.z : 0);
    return dx * dx + dy * dy + dz * dz;
  }

  function currentMappedCount() {
    const availableFaces = state.solid ? Math.max(0, Number(state.solid.faceCount) || 0) : 0;
    return Math.max(0, Math.min(state.positions.length, state.currentFaceCount, availableFaces));
  }

  function shouldSpreadFaceAssignments(mappedCount, totalFaces) {
    if (mappedCount <= 2) {
      return false;
    }
    const emptyFaces = Math.max(0, totalFaces - mappedCount);
    return emptyFaces >= Math.max(4, Math.floor(mappedCount * 0.75));
  }

  function computeMappedFaceIndices(spikes, mappedCount) {
    const totalFaces = Array.isArray(spikes) ? spikes.length : 0;
    const used = Math.max(0, Math.min(totalFaces, Number(mappedCount) || 0));
    if (!used) {
      return [];
    }
    if (!shouldSpreadFaceAssignments(used, totalFaces)) {
      return Array.from({ length: used }, function (_x, idx) {
        return idx;
      });
    }

    const anchors = spikes.map(function (spike) {
      if (spike && spike.tip) {
        return spike.tip;
      }
      if (spike && spike.center) {
        return spike.center;
      }
      return vec(0, 0, 0);
    });
    let centroid = vec(0, 0, 0);
    anchors.forEach(function (pt) {
      centroid = add(centroid, pt);
    });
    centroid = mul(centroid, 1 / Math.max(1, anchors.length));

    let firstIndex = 0;
    let firstDistance = -1;
    for (let i = 0; i < anchors.length; i += 1) {
      const d2 = distanceSquared(anchors[i], centroid);
      if (d2 > firstDistance + 1e-12) {
        firstDistance = d2;
        firstIndex = i;
      }
    }

    const selected = [];
    const selectedSet = new Set();
    const minDistSq = new Array(totalFaces).fill(Number.POSITIVE_INFINITY);

    function selectIndex(idx) {
      selected.push(idx);
      selectedSet.add(idx);
      const p = anchors[idx];
      for (let i = 0; i < totalFaces; i += 1) {
        if (selectedSet.has(i)) {
          continue;
        }
        const d2 = distanceSquared(anchors[i], p);
        if (d2 < minDistSq[i]) {
          minDistSq[i] = d2;
        }
      }
    }

    selectIndex(firstIndex);

    while (selected.length < used) {
      let bestIndex = -1;
      let bestScore = -1;
      for (let i = 0; i < totalFaces; i += 1) {
        if (selectedSet.has(i)) {
          continue;
        }
        const score = minDistSq[i];
        if (score > bestScore + 1e-12) {
          bestScore = score;
          bestIndex = i;
        }
      }
      if (bestIndex < 0) {
        break;
      }
      selectIndex(bestIndex);
    }

    if (selected.length < used) {
      for (let i = 0; i < totalFaces && selected.length < used; i += 1) {
        if (!selectedSet.has(i)) {
          selected.push(i);
          selectedSet.add(i);
        }
      }
    }

    return selected;
  }

  function recomputeMappedFaceIndices() {
    state.mappedFaceIndices = computeMappedFaceIndices(
      state.solid ? state.solid.spikes : [],
      currentMappedCount()
    );
  }

  function spreadPlacementActive() {
    const totalFaces = state.solid ? Math.max(0, Number(state.solid.faceCount) || 0) : 0;
    return shouldSpreadFaceAssignments(currentMappedCount(), totalFaces);
  }

  function ensureSolidMatchesSelection() {
    const nextFaceRequest = faceCountFromSelector();
    if (nextFaceRequest !== state.currentFaceRequest || !state.solid) {
      state.solid = buildStellatedSolid(nextFaceRequest);
      state.currentFaceRequest = nextFaceRequest;
      state.currentFaceCount = Math.max(0, Number(state.solid.faceCount) || 0);
      state.currentSolidName = state.solid.name;
      recomputeMappedFaceIndices();
    }
  }

  function drawScene() {
    if (!state.ctx || !state.solid) {
      return;
    }
    const ctx = state.ctx;
    const w = state.width;
    const h = state.height;
    if (w <= 1 || h <= 1) {
      return;
    }

    ctx.clearRect(0, 0, w, h);

    const mappedCount = currentMappedCount();
    const labelPositions = state.positions.slice(0, mappedCount);
    const faceAssignments =
      Array.isArray(state.mappedFaceIndices) && state.mappedFaceIndices.length >= mappedCount
        ? state.mappedFaceIndices.slice(0, mappedCount)
        : computeMappedFaceIndices(state.solid.spikes, mappedCount);
    const positionScales = computeSpikeScales(labelPositions, mappedCount);
    const spikeScales = computeSpikeScales([], state.solid.faceCount);
    const mappedPositionBySpike = new Array(state.solid.faceCount).fill(null);
    for (let i = 0; i < faceAssignments.length && i < labelPositions.length; i += 1) {
      const faceIdx = Number(faceAssignments[i]);
      if (!Number.isFinite(faceIdx) || faceIdx < 0 || faceIdx >= state.solid.faceCount) {
        continue;
      }
      mappedPositionBySpike[faceIdx] = labelPositions[i];
      if (Number.isFinite(positionScales[i])) {
        spikeScales[faceIdx] = positionScales[i];
      }
    }
    const rotatedTriangles = [];
    const spikeEdges = [];
    const rotatedTips = [];
    const projectedTips = [];

    state.solid.spikes.forEach(function (spike, spikeIndex) {
      const points = spike.points || [];
      if (!points.length) {
        return;
      }
      const mappedPos = mappedPositionBySpike[spikeIndex] || null;
      const spikeTint = faceTintForPosition(mappedPos, state.solid.tint);
      const scale = Number.isFinite(spikeScales[spikeIndex]) ? spikeScales[spikeIndex] : 0.58;
      const apex = add(
        spike.center,
        mul(spike.normal, spike.faceRadius * STELLATION_SCALE * scale)
      );
      const apexRot = rotatePoint(apex, state.angleX, state.angleY);
      const apexProj = project(apexRot, w, h);
      rotatedTips[spikeIndex] = apexRot;
      projectedTips[spikeIndex] = apexProj;

      const baseRot = points.map(function (p) {
        return rotatePoint(p, state.angleX, state.angleY);
      });
      const baseProj = baseRot.map(function (rp) {
        return project(rp, w, h);
      });

      for (let i = 0; i < points.length; i += 1) {
        const nextIndex = (i + 1) % points.length;
        rotatedTriangles.push({
          a: baseProj[i],
          b: baseProj[nextIndex],
          c: apexProj,
          avgZ: (baseRot[i].z + baseRot[nextIndex].z + apexRot.z) / 3,
          shade: spike.shade,
          tint: spikeTint,
        });
        spikeEdges.push({
          a: baseProj[i],
          b: apexProj,
        });
      }
    });

    rotatedTriangles.sort(function (a, b) {
      return a.avgZ - b.avgZ;
    });

    rotatedTriangles.forEach(function (tri) {
      const tint = tri.tint || state.solid.tint;
      const rr = Math.round(clamp(tint[0] * tri.shade, 0, 255));
      const gg = Math.round(clamp(tint[1] * tri.shade, 0, 255));
      const bb = Math.round(clamp(tint[2] * tri.shade, 0, 255));
      ctx.fillStyle = "rgba(" + rr + "," + gg + "," + bb + ",0.72)";
      ctx.beginPath();
      ctx.moveTo(tri.a.x, tri.a.y);
      ctx.lineTo(tri.b.x, tri.b.y);
      ctx.lineTo(tri.c.x, tri.c.y);
      ctx.closePath();
      ctx.fill();
    });

    ctx.strokeStyle = "rgba(6, 10, 22, 0.8)";
    ctx.lineWidth = 1.0;
    state.solid.baseEdges.forEach(function (edge) {
      const a = project(rotatePoint(edge.a, state.angleX, state.angleY), w, h);
      const b = project(rotatePoint(edge.b, state.angleX, state.angleY), w, h);
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.stroke();
    });

    spikeEdges.forEach(function (edge) {
      ctx.beginPath();
      ctx.moveTo(edge.a.x, edge.a.y);
      ctx.lineTo(edge.b.x, edge.b.y);
      ctx.stroke();
    });

    projectedTips.forEach(function (tip, idx) {
      const radius = 1.8 + clamp((spikeScales[idx] || 0.58) * 0.9, 0.5, 2.6);
      ctx.fillStyle = "rgba(232, 246, 255, 0.85)";
      ctx.beginPath();
      ctx.arc(tip.x, tip.y, radius, 0, Math.PI * 2);
      ctx.fill();
    });

    for (let i = 0; i < labelPositions.length; i += 1) {
      const position = labelPositions[i];
      const faceIndex = Number(faceAssignments[i]);
      if (!Number.isFinite(faceIndex) || faceIndex < 0 || faceIndex >= projectedTips.length) {
        continue;
      }
      const tip = projectedTips[faceIndex];
      const tipRot = rotatedTips[faceIndex];
      if (!tip || !tipRot) {
        continue;
      }
      const symbol = String(position.symbol || "").trim().toUpperCase().slice(0, 10);
      if (!symbol) {
        continue;
      }
      const depthAlpha = clamp((tipRot.z + 2.6) / 4.0, 0.25, 1);
      const dx = tip.x - w * 0.5;
      const dy = tip.y - h * 0.5;
      const dl = Math.max(1, Math.hypot(dx, dy));
      const ox = (dx / dl) * 15;
      const oy = (dy / dl) * 15;
      const lx = tip.x + ox;
      const ly = tip.y + oy;

      ctx.strokeStyle = "rgba(200, 230, 255, " + (0.26 * depthAlpha).toFixed(3) + ")";
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(tip.x, tip.y);
      ctx.lineTo(lx, ly);
      ctx.stroke();

      const details = tipLabelDetails(position);
      const symbolFont = "700 12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      const detailFont = "500 10.5px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace";
      const padX = 7;
      const padY = 6;
      const lineGap = 2;
      const symbolLineHeight = 14;
      const detailLineHeight = 12;

      ctx.font = symbolFont;
      let maxTextWidth = ctx.measureText(symbol).width;
      ctx.font = detailFont;
      details.forEach(function (line) {
        const wLine = ctx.measureText(String(line || "")).width;
        if (wLine > maxTextWidth) {
          maxTextWidth = wLine;
        }
      });

      const rw = maxTextWidth + padX * 2;
      const rh = padY * 2 + symbolLineHeight + lineGap + details.length * detailLineHeight;
      drawRoundedRect(ctx, lx - rw * 0.5, ly - rh * 0.5, rw, rh, 6);
      ctx.fillStyle = "rgba(7, 18, 36, " + (0.65 * depthAlpha + 0.2).toFixed(3) + ")";
      ctx.fill();
      ctx.strokeStyle = "rgba(153, 214, 255, " + (0.42 * depthAlpha).toFixed(3) + ")";
      ctx.stroke();

      const left = lx - rw * 0.5 + padX;
      const top = ly - rh * 0.5 + padY;

      ctx.font = symbolFont;
      ctx.fillStyle = "rgba(240, 249, 255, " + Math.max(0.66, depthAlpha).toFixed(3) + ")";
      ctx.fillText(symbol, left, top + 11);

      ctx.font = detailFont;
      ctx.fillStyle = "rgba(213, 229, 244, " + Math.max(0.58, depthAlpha).toFixed(3) + ")";
      for (let li = 0; li < details.length; li += 1) {
        const yy = top + symbolLineHeight + lineGap + li * detailLineHeight + 9;
        ctx.fillText(details[li], left, yy);
      }
    }
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatMetric(position) {
    const weight = Number(position.weight);
    if (Number.isFinite(weight) && weight > 0) {
      return (weight * 100).toFixed(2) + "%";
    }
    const mv = Number(position.market_value_abs);
    if (Number.isFinite(mv) && mv > 0) {
      return "$" + formatCompact(mv);
    }
    const qty = Number(position.quantity_abs);
    if (Number.isFinite(qty) && qty > 0) {
      if (qty >= 1000) {
        return qty.toFixed(0);
      }
      if (qty >= 100) {
        return qty.toFixed(1);
      }
      return qty.toFixed(3);
    }
    return "—";
  }

  function formatTopListMetric(position) {
    if (state.spikeScaleMode === "gain_loss") {
      const pct = positionGainLossPercent(position);
      if (Number.isFinite(pct)) {
        return formatSignedPercent(pct);
      }
      return "—";
    }
    return formatMetric(position);
  }

  function formatCompact(value) {
    const v = Number(value);
    if (!Number.isFinite(v)) {
      return "0";
    }
    const abs = Math.abs(v);
    if (abs >= 1_000_000_000) {
      return (v / 1_000_000_000).toFixed(2) + "b";
    }
    if (abs >= 1_000_000) {
      return (v / 1_000_000).toFixed(2) + "m";
    }
    if (abs >= 1_000) {
      return (v / 1_000).toFixed(1) + "k";
    }
    return v.toFixed(2);
  }

  function formatQuantity(value) {
    const v = Number(value);
    if (!Number.isFinite(v)) {
      return "—";
    }
    if (Math.abs(v) >= 10000) {
      return v.toFixed(0);
    }
    if (Math.abs(v) >= 1000) {
      return v.toFixed(1);
    }
    if (Math.abs(v) >= 100) {
      return v.toFixed(2);
    }
    if (Math.abs(v) >= 10) {
      return v.toFixed(3);
    }
    return v.toFixed(4);
  }

  function formatMoney(value) {
    const v = Number(value);
    if (!Number.isFinite(v)) {
      return "—";
    }
    const abs = Math.abs(v);
    if (abs >= 1_000_000_000) {
      return "$" + (v / 1_000_000_000).toFixed(2) + "b";
    }
    if (abs >= 1_000_000) {
      return "$" + (v / 1_000_000).toFixed(2) + "m";
    }
    if (abs >= 1_000) {
      return "$" + (v / 1_000).toFixed(2) + "k";
    }
    return "$" + v.toFixed(2);
  }

  function formatPrice(value) {
    const v = Number(value);
    if (!Number.isFinite(v)) {
      return "—";
    }
    if (Math.abs(v) >= 1000) {
      return "$" + v.toFixed(2);
    }
    if (Math.abs(v) >= 100) {
      return "$" + v.toFixed(3);
    }
    if (Math.abs(v) >= 1) {
      return "$" + v.toFixed(4);
    }
    return "$" + v.toFixed(6);
  }

  function formatSignedMoney(value) {
    const v = Number(value);
    if (!Number.isFinite(v)) {
      return "—";
    }
    if (v > 0) {
      return "+" + formatMoney(v);
    }
    if (v < 0) {
      return "-" + formatMoney(Math.abs(v));
    }
    return "$0.00";
  }

  function formatSignedPercent(value) {
    const v = Number(value);
    if (!Number.isFinite(v)) {
      return "—";
    }
    const pct = (Math.abs(v) * 100).toFixed(2) + "%";
    if (v > 0) {
      return "+" + pct;
    }
    if (v < 0) {
      return "-" + pct;
    }
    return "0.00%";
  }

  function tipLabelDetails(position) {
    const lastPrice = formatPrice(position && position.last_price);
    const quantity = formatQuantity(position && position.quantity_signed);
    const value = formatMoney(position && position.market_value_signed);
    const plDollar = formatSignedMoney(position && position.gain_loss_dollar);
    const plPercent = formatSignedPercent(positionGainLossPercent(position));
    const dayPercent = formatSignedPercent(position && position.day_change_percent);
    return [
      "Last " + lastPrice,
      "Sh " + quantity,
      "Eq " + value,
      "P/L " + plDollar + " " + plPercent,
      "Day " + dayPercent,
    ];
  }

  function renderTopList() {
    const maxCandidates = Math.min(20, state.positions.length);
    const shown = state.positions.slice(0, maxCandidates);
    const mappedCount = currentMappedCount();
    const mappedFaces = Array.isArray(state.mappedFaceIndices) ? state.mappedFaceIndices : [];
    if (!shown.length) {
      topListEl.className = "portfolio-solid-toplist";
      topListEl.innerHTML = "<div class='small'>No positions available to map to spikes.</div>";
      return;
    }
    const rows = shown.map(function (p, idx) {
      const symbolRaw = normalizeSymbol(p && p.symbol);
      const symbol = escapeHtml(symbolRaw);
      const isMapped = idx < mappedCount;
      const metric = escapeHtml(formatTopListMetric(p));
      const upDisabled = idx <= 0 ? " disabled" : "";
      const downDisabled = idx >= (state.positions.length - 1) ? " disabled" : "";
      const faceIdx = Number(mappedFaces[idx]);
      const slotLabel = isMapped && Number.isFinite(faceIdx) && faceIdx >= 0
        ? ("F" + (faceIdx + 1))
        : "—";
      return (
        "<div class='portfolio-solid-row" + (isMapped ? " mapped" : "") + "'>" +
        "<span class='rank'>#" + (idx + 1) + "</span>" +
        "<span class='symbol'>" + symbol + "</span>" +
        "<span class='weight'>" + metric + "</span>" +
        "<span class='slot'>" + slotLabel + "</span>" +
        "<span class='controls'>" +
        "<button class='solid-move-btn' type='button' data-action='up' data-symbol='" + symbol + "'" + upDisabled + ">↑</button>" +
        "<button class='solid-move-btn' type='button' data-action='down' data-symbol='" + symbol + "'" + downDisabled + ">↓</button>" +
        "</span>" +
        "</div>"
      );
    });
    topListEl.className = "portfolio-solid-toplist";
    topListEl.innerHTML = rows.join("");
  }

  function updateText() {
    recomputeMappedFaceIndices();
    const shownCount = currentMappedCount();
    const orderMode = state.manualOrderSymbols.length ? "Custom" : "Ranked";
    const faceMode = String(faceSelect.value || "auto").toLowerCase();
    const depthMode = state.spikeScaleMode === "gain_loss" ? "Gain/Loss %" : "Portfolio weight";
    const placementMode = spreadPlacementActive() ? "Spread" : "Sequential";
    const requestedLabel = state.currentFaceRequest !== state.currentFaceCount
      ? (state.currentFaceRequest + " -> " + state.currentFaceCount)
      : String(state.currentFaceCount);
    summaryEl.textContent =
      state.currentSolidName +
      " (" +
      state.currentFaceCount +
      " faces) · showing top " +
      shownCount +
      " of " +
      state.totalPositions +
      " holdings · depth: " +
      depthMode;
    const when = state.lastUpdated
      ? new Date(state.lastUpdated).toLocaleTimeString()
      : "not yet";
    statusEl.textContent =
      "Mode: " +
      (faceMode === "auto" ? "Auto" : ("Manual " + requestedLabel)) +
      " · Spike depth: " +
      depthMode +
      " · Placement: " +
      placementMode +
      " · Ticker order: " +
      orderMode +
      " · Auto tier: " +
      state.autoFaceCount +
      " faces · last update " +
      when;
    if (scaleHintEl) {
      scaleHintEl.textContent = state.spikeScaleMode === "gain_loss"
        ? "Use the up/down controls in the ticker list to assign tickers to different spike points. Spike distance from center scales by signed gain/loss percent. When many faces are empty, tickers auto-spread across the solid. For more than 20 faces, select Custom and enter a larger face target."
        : "Use the up/down controls in the ticker list to assign tickers to different spike points. Spike depth scales by relative portfolio weight. When many faces are empty, tickers auto-spread across the solid. For more than 20 faces, select Custom and enter a larger face target.";
    }
    renderTopList();
  }

  function resizeCanvas() {
    const rect = canvas.getBoundingClientRect();
    const cssWidth = Math.max(240, Math.floor(rect.width));
    const cssHeight = Math.max(240, Math.floor(rect.height));
    const dpr = Math.max(1, window.devicePixelRatio || 1);

    state.dpr = dpr;
    state.width = cssWidth;
    state.height = cssHeight;

    canvas.width = Math.floor(cssWidth * dpr);
    canvas.height = Math.floor(cssHeight * dpr);
    canvas.style.width = cssWidth + "px";
    canvas.style.height = cssHeight + "px";
    state.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  async function fetchPositions() {
    if (state.pendingFetch) {
      return;
    }
    state.pendingFetch = true;
    statusEl.textContent = "Refreshing portfolio positions…";

    try {
      const resp = await fetch("/api/portfolio/positions/top?limit=200", {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      const payload = await resp.json();
      if (!resp.ok) {
        throw new Error(payload.error || ("HTTP " + resp.status));
      }

      const rows = Array.isArray(payload.positions) ? payload.positions : [];
      state.rawPositions = rows.filter(function (row) {
        return row && typeof row.symbol === "string" && row.symbol.trim() !== "";
      });
      state.positions = applyManualOrder(state.rawPositions);

      if (state.manualOrderSymbols.length) {
        const valid = new Set(
          state.positions.map(function (row) {
            return normalizeSymbol(row && row.symbol);
          })
        );
        state.manualOrderSymbols = state.manualOrderSymbols
          .map(normalizeSymbol)
          .filter(function (sym, idx, arr) {
            return !!sym && valid.has(sym) && arr.indexOf(sym) === idx;
          });
      }

      state.totalPositions = Number(payload.total_positions);
      if (!Number.isFinite(state.totalPositions) || state.totalPositions < 0) {
        state.totalPositions = state.positions.length;
      }

      const autoFace = Number(payload.auto_face_count);
      state.autoFaceCount = FACE_LEVELS.indexOf(autoFace) >= 0 ? autoFace : chooseFaceLevel(state.totalPositions);

      ensureSolidMatchesSelection();
      state.lastUpdated = Date.now();
      updateText();
    } catch (err) {
      const message = err && err.message ? err.message : String(err || "unknown error");
      statusEl.textContent = "Unable to load portfolio positions: " + message;
    } finally {
      state.pendingFetch = false;
    }
  }

  function animate() {
    state.angleY += 0.003;
    state.angleX += 0.0012;
    drawScene();
    state.frameHandle = window.requestAnimationFrame(animate);
  }

  function init() {
    loadManualOrder();
    loadScaleMode();
    loadCustomFaceRequest();
    syncFaceCustomInputState();
    resizeCanvas();
    ensureSolidMatchesSelection();
    updateText();

    faceSelect.addEventListener("change", function () {
      syncFaceCustomInputState();
      ensureSolidMatchesSelection();
      updateText();
    });

    if (faceCustomInput) {
      faceCustomInput.addEventListener("change", function () {
        saveCustomFaceRequest();
        if (!isCustomFaceMode()) {
          return;
        }
        ensureSolidMatchesSelection();
        updateText();
      });
      faceCustomInput.addEventListener("blur", function () {
        saveCustomFaceRequest();
      });
      faceCustomInput.addEventListener("keydown", function (evt) {
        if (evt && evt.key === "Enter") {
          saveCustomFaceRequest();
          if (!isCustomFaceMode()) {
            return;
          }
          ensureSolidMatchesSelection();
          updateText();
        }
      });
    }

    if (scaleModeSelect) {
      scaleModeSelect.addEventListener("change", function () {
        state.spikeScaleMode = normalizeScaleMode(scaleModeSelect.value);
        scaleModeSelect.value = state.spikeScaleMode;
        saveScaleMode();
        updateText();
      });
    }

    if (refreshBtn) {
      refreshBtn.addEventListener("click", function () {
        fetchPositions();
      });
    }

    if (resetOrderBtn) {
      resetOrderBtn.addEventListener("click", function () {
        state.manualOrderSymbols = [];
        saveManualOrder();
        state.positions = applyManualOrder(state.rawPositions);
        updateText();
      });
    }

    topListEl.addEventListener("click", function (evt) {
      const btn = evt.target && evt.target.closest ? evt.target.closest("button.solid-move-btn") : null;
      if (!btn) {
        return;
      }
      const action = String(btn.getAttribute("data-action") || "").toLowerCase();
      const symbol = String(btn.getAttribute("data-symbol") || "");
      if ((action === "up" || action === "down") && moveSymbol(symbol, action)) {
        updateText();
      }
    });

    if (window.ResizeObserver) {
      const ro = new ResizeObserver(function () {
        resizeCanvas();
      });
      ro.observe(canvas.parentElement || canvas);
    } else {
      window.addEventListener("resize", resizeCanvas);
    }

    fetchPositions();
    state.refreshHandle = window.setInterval(fetchPositions, 20000);
    animate();
  }

  init();
})();
