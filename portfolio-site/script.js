const menuToggle = document.getElementById("menuToggle");
const mainNav = document.getElementById("mainNav");

menuToggle.addEventListener("click", () => {
  const isOpen = mainNav.classList.toggle("open");
  menuToggle.setAttribute("aria-expanded", String(isOpen));
  menuToggle.textContent = isOpen ? "×" : "☰";
});

mainNav.querySelectorAll("a").forEach((link) => {
  link.addEventListener("click", () => {
    mainNav.classList.remove("open");
    menuToggle.setAttribute("aria-expanded", "false");
    menuToggle.textContent = "☰";
  });
});


/* Interactive pipeline walkthrough: explanatory content only, no fake live metrics. */
const stageContent = {
  ingest: {kicker:"STAGE 01 / INGESTION",title:"Bring source data in safely.",description:"Load synthetic customer and transaction files, including an incremental batch, as the inputs to the pipeline.",focus:"Repeatable input handling",output:"Raw source files ready for processing",tech:"Python · CSV · AWS S3"},
  transform: {kicker:"STAGE 02 / PROCESSING",title:"Transform data with Spark.",description:"Apply batch and incremental processing to the synthetic banking data using the project's PySpark pipeline.",focus:"Consistent, repeatable transformations",output:"Processed customer and transaction datasets",tech:"Python · PySpark · Spark SQL"},
  quality: {kicker:"STAGE 03 / DATA QUALITY",title:"Catch issues before downstream use.",description:"Run data-quality checks and anomaly screening, then retain a machine-readable report for the run.",focus:"Validation and anomaly screening",output:"Quality results and diagnostic evidence",tech:"Python · Data quality checks"},
  reconcile: {kicker:"STAGE 04 / RECONCILIATION",title:"Compare results across processing paths.",description:"Use reconciliation checks to help identify mismatches between pipeline outputs and expected results.",focus:"Cross-engine and output consistency",output:"Reconciliation outcome",tech:"SQL · Spark · Reconciliation checks"},
  operate: {kicker:"STAGE 05 / ORCHESTRATION",title:"Make the run observable and repeatable.",description:"Run the pipeline through Docker-based Airflow orchestration, validate the generated report, and upload raw inputs to S3.",focus:"Orchestration, status validation and recovery",output:"Pipeline run report and S3 ingestion result",tech:"Docker · Apache Airflow · AWS S3"}
};
const stageButtons = Array.from(document.querySelectorAll(".stage-button"));
const stagePanel = document.getElementById("stage-detail");
stageButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const data = stageContent[button.dataset.stage];
    if (!data) return;
    stageButtons.forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-selected", String(active));
    });
    stagePanel.setAttribute("aria-labelledby", button.id);
    ["kicker","title","description","focus","output","tech"].forEach((key) => {
      const node = document.getElementById("stage-" + key);
      if (node) node.textContent = data[key];
    });
  });
});

/* Project filters keep the public work scannable for technical recruiters. */
const filterButtons = Array.from(document.querySelectorAll(".filter-button"));
const projectCards = Array.from(document.querySelectorAll(".project-grid .project-card"));
filterButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const filter = button.dataset.filter;
    filterButtons.forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      item.setAttribute("aria-pressed", String(active));
    });
    projectCards.forEach((card) => {
      const categories = (card.dataset.categories || "").split(/\\s+/);
      card.hidden = filter !== "all" && !categories.includes(filter);
    });
  });
});


/* Build-refreshed dataset metrics and interactive illustrative scale chart. */
(async function initializeScaleDashboard() {
  const countNode = document.getElementById("metric-customers");
  const txNode = document.getElementById("metric-transactions");
  const footprintNode = document.getElementById("metric-footprint");
  const updatedNode = document.getElementById("metric-updated");
  const chart = document.getElementById("dataset-chart");
  const formatCount = (n) => new Intl.NumberFormat("en-US").format(n);
  const formatBytes = (n) => n >= 1e9 ? (n / 1e9).toFixed(2) + " GB" : n >= 1e6 ? (n / 1e6).toFixed(2) + " MB" : n >= 1e3 ? (n / 1e3).toFixed(1) + " KB" : n + " B";
  const svgEl = (tag, attrs, text) => {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs || {}).forEach(([k,v]) => el.setAttribute(k, String(v)));
    if (text !== undefined) el.textContent = text;
    return el;
  };
  function drawChart(customers, transactions) {
    if (!chart) return;
    chart.replaceChildren();
    chart.setAttribute("viewBox","0 0 600 220");
    chart.append(svgEl("text",{x:18,y:24,fill:"#9aa7c1","font-size":11},"Generated rows by dataset"));
    const values=[{label:"Customers",value:customers,color:"#a99bff"},{label:"Transactions",value:transactions,color:"#6bdcf2"}];
    const max=Math.max(1,...values.map(v=>v.value));
    values.forEach((v,i)=>{
      const y=54+i*76, width=Math.max(3,Math.round(510*v.value/max));
      chart.append(svgEl("text",{x:18,y:y,fill:"#b7c5dc","font-size":12},v.label));
      chart.append(svgEl("rect",{x:18,y:y+12,width:510,height:22,rx:6,fill:"#1b263b"}));
      chart.append(svgEl("rect",{x:18,y:y+12,width, height:22,rx:6,fill:v.color,opacity:.92}));
      chart.append(svgEl("text",{x:548,y:y+28,fill:"#eef3ff","font-size":12,"text-anchor":"end"},formatCount(v.value)));
    });
    chart.append(svgEl("text",{x:18,y:211,fill:"#7184a4","font-size":9},"Counts measured from generated CSV files"));
  }
  try {
    const response = await fetch("data/pipeline-metrics.json?build=" + Date.now(), {cache:"no-store"});
    if (!response.ok) throw new Error("metrics unavailable");
    const metrics = await response.json();
    if (countNode) countNode.textContent = formatCount(metrics.customers.rows);
    if (txNode) txNode.textContent = formatCount(metrics.transactions.rows);
    if (footprintNode) footprintNode.textContent = formatBytes(metrics.total_bytes);
    if (updatedNode) updatedNode.textContent = "Generated from CSV files during deployment · " + new Date(metrics.generated_at).toLocaleString();
    drawChart(metrics.customers.rows, metrics.transactions.rows);
  } catch (_) {
    if (countNode) countNode.textContent = "Unavailable";
    if (txNode) txNode.textContent = "Unavailable";
    if (footprintNode) footprintNode.textContent = "Unavailable";
    if (updatedNode) updatedNode.textContent = "Metrics could not be loaded. The scale explorer remains illustrative.";
    if (chart) {
      chart.replaceChildren();
      chart.append(svgEl("text",{x:18,y:30,fill:"#9aa7c1","font-size":12},"Build metrics unavailable"));
    }
  }

  const slider=document.getElementById("scale-records");
  const label=document.getElementById("scale-record-label");
  const estimate=document.getElementById("scale-estimate");
  const prettyCount=(n)=>n>=1e9?(n/1e9).toFixed(n%1e9===0?0:1)+" billion":n>=1e6?(n/1e6).toFixed(n%1e6===0?0:1)+" million":n>=1e3?(n/1e3).toFixed(n%1e3===0?0:1)+" thousand":String(n);
  const prettySize=(bytes)=>bytes>=1e15?(bytes/1e15).toFixed(2)+" PB":bytes>=1e12?(bytes/1e12).toFixed(2)+" TB":bytes>=1e9?(bytes/1e9).toFixed(2)+" GB":bytes>=1e6?(bytes/1e6).toFixed(2)+" MB":(bytes/1e3).toFixed(1)+" KB";
  function updateEstimate(){
    if(!slider)return;
    const records=Number(slider.value);
    if(label)label.textContent=prettyCount(records);
    if(estimate)estimate.textContent="~"+prettySize(records*1000);
  }
  if(slider){slider.addEventListener("input",updateEstimate);updateEstimate();}
})();
