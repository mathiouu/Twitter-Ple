
<script>
import { Bar } from "vue-chartjs";
export default {
  extends: Bar,
  props: {
    labels: {
      type: Array,
      default: null,
    },
    data: {
      type: Array,
      default: null,
    },
  },
  data: () => ({
    chartdata: {
      labels: [],
      datasets: [
        {
          label: "Data One",
          backgroundColor: "#f87979",
          data: [],
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
    },
  }),

  mounted() {
    this.chartdata.labels = this.labels;
    this.chartdata.datasets[0].data = this.data;
    console.log(this.chartdata);
    this.renderChart(this.chartdata, this.options);
  },
};
</script>